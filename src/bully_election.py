#!/usr/bin/env python3
# bully_election.py
import asyncio
import logging
import random
from typing import Dict, Set, Optional

import grpc
from google.protobuf.empty_pb2 import Empty

from BullyElection_pb2 import (
    CandidateMessage,
    LeaderMessage,
    RequestMessage,
    ResponseMessage,
    ElectionMessage,
)
from BullyElection_pb2_grpc import BullyElectionServicer, BullyElectionStub

logging.basicConfig(level=logging.INFO)

# tuning knobs
ELECTION_TIMEOUT = 2.0
LEADER_ANNOUNCE_DELAY = 1.0
LEADER_CHECK_INTERVAL = 5.0
ELECTION_COOLDOWN = 3.0
PER_PEER_DELAY_MIN = 0.05
PER_PEER_DELAY_MAX = 0.1


class BullyElectionImpl(BullyElectionServicer):
    """
    Bully election algorithm implementation following Garcia-Molina [1982].

    Key principles:
    - Process with highest priority always wins
    - Election messages sent only to higher-priority processes
    - If no response from higher processes, declare self as coordinator
    - Coordinator messages sent to all lower-priority processes
    """

    def __init__(
        self,
        server_id: str,  # logical server ID (srv-A, srv-B, etc.)
        priority: int,
        known_servers: dict | None = None,
        self_grpc_addr: str | None = None,
    ):
        self.server_id = server_id
        self.priority = priority
        self.known_servers: Dict[str, str] = dict(known_servers or {})
        self.self_grpc_addr = self_grpc_addr

        self.leader: Optional[str] = None
        self._election_in_progress = False
        self._lock = asyncio.Lock()
        self._last_election_time = 0

        logging.info(
            "BullyElectionImpl created: server_id=%s priority=%s known=%s self_addr=%s",
            self.server_id,
            self.priority,
            list(self.known_servers.keys()),
            self.self_grpc_addr,
        )

    # ---- RPC handlers ---------------------------------------------------
    async def Election(self, request: ElectionMessage, context) -> ResponseMessage:
        """
        Handle election message from lower-priority process.
        Send 'alive' response and start our own election if not already running.
        """
        logging.info(
            "%s: Election RPC received from lower-priority process", self.server_id
        )

        # Send alive message back to sender
        response = ResponseMessage(priority=self.priority)

        # Start our own election (higher priority process should take over)
        asyncio.create_task(self._start_election_if_needed())

        return response

    async def Leader(self, request: LeaderMessage, context) -> Empty:
        """Handle coordinator announcement from new leader"""
        logging.info(
            "%s: Leader RPC received, new leader=%s", self.server_id, request.leader_id
        )

        async with self._lock:
            self.leader = request.leader_id
            self._election_in_progress = False

        return Empty()

    async def Request(self, request: RequestMessage, context) -> ResponseMessage:
        """Handle heartbeat/health check request"""
        logging.debug("%s: Request RPC received (heartbeat)", self.server_id)
        return ResponseMessage(priority=self.priority)

    # ---- Helper methods -------------------------------------------------
    def _get_higher_priority_servers(self) -> Dict[str, str]:
        """Get servers with higher priority than this one"""
        higher_servers = {}
        for srv_id, addr in self.known_servers.items():
            if srv_id == self.server_id or addr == self.self_grpc_addr:
                continue
            # In our topology, we need to get actual priorities
            # For now, assume srv-A=high, srv-B=medium, srv-C=low based on naming
            server_priority = self._get_server_priority(srv_id)
            if server_priority > self.priority:
                higher_servers[srv_id] = addr
        return higher_servers

    def _get_lower_priority_servers(self) -> Dict[str, str]:
        """Get servers with lower priority than this one"""
        lower_servers = {}
        for srv_id, addr in self.known_servers.items():
            if srv_id == self.server_id or addr == self.self_grpc_addr:
                continue
            server_priority = self._get_server_priority(srv_id)
            if server_priority < self.priority:
                lower_servers[srv_id] = addr
        return lower_servers

    def _get_server_priority(self, server_id: str) -> int:
        """
        Map server IDs to priorities based on the known topology.
        This should ideally come from server registration, but for now we'll
        derive it from the server startup parameters.
        """
        # This is a simplified mapping - in real implementation,
        # you'd want to track actual priorities of each server
        priority_map = {
            "srv-A": 9,  # Highest priority
            "srv-B": 5,  # Medium priority
            "srv-C": 1,  # Lowest priority
        }
        return priority_map.get(server_id, 0)

    async def _send_election_messages(self) -> bool:
        """
        Send election messages to all higher-priority processes.
        Returns True if any process responded (meaning we should not become leader).
        """
        higher_servers = self._get_higher_priority_servers()

        if not higher_servers:
            logging.info(
                "%s: no higher-priority servers, can become leader", self.server_id
            )
            return False

        logging.info(
            "%s: sending election messages to higher-priority servers: %s",
            self.server_id,
            list(higher_servers.keys()),
        )

        responses = []
        for srv_id, addr in higher_servers.items():
            try:
                async with grpc.aio.insecure_channel(addr) as ch:
                    stub = BullyElectionStub(ch)
                    response = await asyncio.wait_for(
                        stub.Election(ElectionMessage()), timeout=ELECTION_TIMEOUT
                    )
                    logging.info(
                        "%s: received alive response from %s (priority=%s)",
                        self.server_id,
                        srv_id,
                        response.priority,
                    )
                    responses.append(True)
            except Exception as exc:
                logging.info(
                    "%s: no response from %s (%s): %s",
                    self.server_id,
                    srv_id,
                    addr,
                    type(exc).__name__,
                )
                responses.append(False)

            await asyncio.sleep(random.uniform(PER_PEER_DELAY_MIN, PER_PEER_DELAY_MAX))

        # If any higher-priority process responded, we should not become leader
        any_alive = any(responses)
        logging.info(
            "%s: election phase result - higher processes alive: %s",
            self.server_id,
            any_alive,
        )

        return any_alive

    async def _announce_leadership(self):
        """Send coordinator messages to all lower-priority processes"""
        lower_servers = self._get_lower_priority_servers()

        if not lower_servers:
            logging.info("%s: no lower-priority servers to inform", self.server_id)
            return

        logging.info(
            "%s: announcing leadership to lower-priority servers: %s",
            self.server_id,
            list(lower_servers.keys()),
        )

        success_count = 0
        for srv_id, addr in lower_servers.items():
            try:
                async with grpc.aio.insecure_channel(addr) as ch:
                    stub = BullyElectionStub(ch)
                    await asyncio.wait_for(
                        stub.Leader(LeaderMessage(leader_id=self.server_id)),
                        timeout=ELECTION_TIMEOUT,
                    )
                    success_count += 1
                    logging.info(
                        "%s: informed %s of new leadership", self.server_id, srv_id
                    )
            except Exception as exc:
                logging.info(
                    "%s: failed to inform %s: %s",
                    self.server_id,
                    srv_id,
                    type(exc).__name__,
                )

            await asyncio.sleep(random.uniform(PER_PEER_DELAY_MIN, PER_PEER_DELAY_MAX))

        logging.info(
            "%s: leadership announcement completed, informed %d servers",
            self.server_id,
            success_count,
        )

    async def _start_election_if_needed(self):
        """Start election if not already in progress and enough time has passed"""
        current_time = asyncio.get_event_loop().time()

        async with self._lock:
            if self._election_in_progress:
                logging.debug("%s: election already in progress", self.server_id)
                return

            if current_time - self._last_election_time < ELECTION_COOLDOWN:
                logging.debug("%s: election cooldown in effect", self.server_id)
                return

            self._election_in_progress = True
            self._last_election_time = current_time

        try:
            await self._run_election()
        finally:
            async with self._lock:
                self._election_in_progress = False

    async def _run_election(self):
        """Core election algorithm implementation"""
        logging.info(
            "%s: starting bully election (priority=%s)", self.server_id, self.priority
        )

        # Phase 1: Send election messages to higher-priority processes
        higher_processes_alive = await self._send_election_messages()

        if higher_processes_alive:
            # Some higher-priority process is alive, they will handle leadership
            logging.info(
                "%s: higher-priority processes alive, waiting for their decision",
                self.server_id,
            )
            return

        # Phase 2: No higher-priority process responded, become leader
        logging.info(
            "%s: no higher-priority processes responded, becoming leader",
            self.server_id,
        )

        # Wait a moment to avoid race conditions
        await asyncio.sleep(LEADER_ANNOUNCE_DELAY)

        async with self._lock:
            self.leader = self.server_id

        # Phase 3: Announce leadership to lower-priority processes
        await self._announce_leadership()

        logging.info("%s: successfully became leader", self.server_id)

    async def _check_leader_health(self) -> bool:
        """Check if current leader is still alive"""
        if not self.leader:
            return False

        if self.leader == self.server_id:
            return True  # We are the leader

        leader_addr = self.known_servers.get(self.leader)
        if not leader_addr:
            logging.info(
                "%s: leader %s not in known servers", self.server_id, self.leader
            )
            return False

        try:
            async with grpc.aio.insecure_channel(leader_addr) as ch:
                stub = BullyElectionStub(ch)
                response = await asyncio.wait_for(
                    stub.Request(RequestMessage()), timeout=ELECTION_TIMEOUT
                )
                logging.debug(
                    "%s: leader %s is healthy (priority=%s)",
                    self.server_id,
                    self.leader,
                    response.priority,
                )
                return True
        except Exception as exc:
            logging.info(
                "%s: leader %s health check failed: %s",
                self.server_id,
                self.leader,
                type(exc).__name__,
            )
            return False

    # ---- External APIs -------------------------------------------------
    async def StartElection(self):
        """Manually trigger an election (e.g., on startup or leader failure)"""
        logging.info("%s: manually starting election", self.server_id)
        await self._start_election_if_needed()

    async def LifeCycle(self):
        """Main lifecycle loop - monitors leader and triggers elections as needed"""
        logging.info("%s: starting bully election lifecycle", self.server_id)

        # Initial election on startup
        await self._start_election_if_needed()

        while True:
            try:
                # Check if we have a leader
                if not self.leader:
                    logging.info(
                        "%s: no leader known, starting election", self.server_id
                    )
                    await self._start_election_if_needed()
                    await asyncio.sleep(ELECTION_COOLDOWN)
                    continue

                # Check if we're in an election
                async with self._lock:
                    election_in_progress = self._election_in_progress

                if election_in_progress:
                    logging.debug("%s: election in progress, waiting", self.server_id)
                    await asyncio.sleep(ELECTION_COOLDOWN)
                    continue

                # Check leader health
                leader_healthy = await self._check_leader_health()
                if not leader_healthy:
                    logging.info(
                        "%s: leader %s appears to have failed, starting election",
                        self.server_id,
                        self.leader,
                    )
                    async with self._lock:
                        self.leader = None
                    await self._start_election_if_needed()
                    await asyncio.sleep(ELECTION_COOLDOWN)
                else:
                    # Leader is healthy, wait before next check
                    await asyncio.sleep(LEADER_CHECK_INTERVAL)

            except asyncio.CancelledError:
                logging.info("%s: lifecycle cancelled", self.server_id)
                raise
            except Exception as exc:
                logging.exception("%s: lifecycle error, retrying", self.server_id)
                await asyncio.sleep(ELECTION_COOLDOWN)

    def get_current_leader(self) -> Optional[str]:
        """Get the current leader (thread-safe)"""
        return self.leader

    def is_leader(self) -> bool:
        """Check if this node is the current leader"""
        return self.leader == self.server_id

    def get_status(self) -> dict:
        """Get current election status"""
        return {
            "server_id": self.server_id,
            "priority": self.priority,
            "current_leader": self.leader,
            "is_leader": self.is_leader(),
            "election_in_progress": self._election_in_progress,
            "known_servers": list(self.known_servers.keys()),
        }
