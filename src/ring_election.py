#!/usr/bin/env python3
# ring_election.py
import asyncio
import logging
import random
from typing import Dict, List, Optional

import grpc
from google.protobuf.empty_pb2 import Empty
from collections import OrderedDict

from RingElection_pb2 import (
    ElectionMessage,
    LeaderMessage,
    RequestMessage,
    ResponseMessage,
)
from RingElection_pb2_grpc import (
    RingElectionServicer,
    RingElectionStub,
)

logging.basicConfig(level=logging.INFO)

# tuning knobs
ELECTION_TIMEOUT = 2.0
ELECTION_ROUND_INTERVAL = 3.0
LEADER_CHECK_INTERVAL = 5.0  # how often to ping leader when stable
PER_PEER_DELAY_MIN = 0.05
PER_PEER_DELAY_MAX = 0.1


class RingElectionImpl(RingElectionServicer):
    """
    Ring-based election algorithm implementation.

    Implements the classic ring election algorithm where:
    - Processes are organized in a logical ring
    - Election messages circulate clockwise containing priority numbers
    - Process with highest priority becomes coordinator
    - Coordinator messages inform all processes of the new leader
    """

    def __init__(
        self,
        server_id: str,  # logical server ID (srv-A, srv-B, etc.)
        priority: int,
        leader_addr: str,
        known_servers: dict | None = None,
        self_grpc_addr: str | None = None,
    ):
        self.server_id = server_id  # logical ID in ring (srv-A, srv-B, etc.)
        self.priority = priority
        self.leader_addr = leader_addr
        # ensure insertion order is preserved for ring topology
        self.known_servers: "OrderedDict[str, str]" = OrderedDict(known_servers or {})
        self.self_grpc_addr = self_grpc_addr
        self.leader: Optional[str] = None
        self._lock = asyncio.Lock()
        self._election_in_progress = False

        logging.info(
            "RingElectionImpl created: server_id=%s priority=%s known=%s self_addr=%s",
            self.server_id,
            self.priority,
            list(self.known_servers.keys()),
            self.self_grpc_addr,
        )

    # ---- RPC handlers ---------------------------------------------------
    async def Election(self, request: ElectionMessage, context) -> Empty:
        logging.info(
            "%s: Election RPC received (originator=%s) nodes=%s priorities=%s",
            self.server_id,
            request.originator_id,
            list(request.node_ids),
            list(request.priorities),
        )

        # If the election message has returned to originator -> decide winner & broadcast
        if request.originator_id == self.server_id:
            if not request.priorities or not request.node_ids:
                logging.warning(
                    "%s: Election message returned empty - electing self",
                    self.server_id,
                )
                leader_id = self.server_id
            else:
                # Find node with highest priority
                max_priority = max(request.priorities)
                max_indices = [
                    i for i, p in enumerate(request.priorities) if p == max_priority
                ]

                # If tie, choose lexicographically smallest server_id for consistency
                if len(max_indices) > 1:
                    winner_idx = min(max_indices, key=lambda i: request.node_ids[i])
                else:
                    winner_idx = max_indices[0]

                leader_id = request.node_ids[winner_idx]

            logging.info(
                "%s: Election completed, chosen leader=%s (priority=%s)",
                self.server_id,
                leader_id,
                max(request.priorities) if request.priorities else self.priority,
            )

            async with self._lock:
                self.leader = leader_id
                self._election_in_progress = False

            await self._broadcast_leader(leader_id, request.originator_id)
            return Empty()

        # Otherwise append our info and forward to next reachable successor
        new_msg = ElectionMessage(
            originator_id=request.originator_id,
            node_ids=list(request.node_ids) + [self.server_id],
            priorities=list(request.priorities) + [int(self.priority)],
        )

        forwarded = await self._forward_election(new_msg)
        if not forwarded:
            logging.info(
                "%s: could not forward election; electing self as leader",
                self.server_id,
            )
            async with self._lock:
                self.leader = self.server_id
                self._election_in_progress = False
            await self._broadcast_leader(self.server_id, request.originator_id)

        return Empty()

    async def Leader(self, request: LeaderMessage, context) -> Empty:
        logging.info(
            "%s: Leader RPC received leader=%s originator=%s",
            self.server_id,
            request.leader_id,
            request.originator_id,
        )

        async with self._lock:
            self.leader = request.leader_id
            self._election_in_progress = False

        # If message returned to originator, stop propagation
        if request.originator_id == self.server_id:
            logging.info(
                "%s: Leader message returned to originator, stopping propagation",
                self.server_id,
            )
            return Empty()

        # Forward to next node in ring
        await self._forward_leader(request)
        return Empty()

    async def Request(self, request: RequestMessage, context) -> ResponseMessage:
        logging.debug("%s: Request RPC received (leader health check)", self.server_id)
        return ResponseMessage(priority=int(self.priority))

    # ---- Ring topology helpers ------------------------------------------
    def _get_ring_order(self) -> List[str]:
        """Return the logical ring order (srv-A, srv-B, srv-C, etc.)"""
        return list(self.known_servers.keys())

    def _get_next_in_ring(self, current_id: str) -> str:
        """Get the next server in ring after current_id"""
        ring = self._get_ring_order()
        try:
            current_idx = ring.index(current_id)
            return ring[(current_idx + 1) % len(ring)]
        except ValueError:
            # If current_id not in ring, return first node
            return ring[0] if ring else current_id

    def _get_reachable_successors(self, start_id: str) -> List[str]:
        """Get list of successors in ring order, excluding start_id"""
        ring = self._get_ring_order()
        if not ring or len(ring) <= 1:
            return []

        try:
            start_idx = ring.index(start_id)
        except ValueError:
            return ring  # If start_id not found, try all nodes

        successors = []
        for i in range(1, len(ring)):  # Start from next node, exclude self
            successor_idx = (start_idx + i) % len(ring)
            successors.append(ring[successor_idx])

        return successors

    # ---- Message forwarding ---------------------------------------------
    async def _forward_election(self, msg: ElectionMessage) -> bool:
        """Forward election message to next reachable successor in ring"""
        successors = self._get_reachable_successors(self.server_id)

        for successor_id in successors:
            addr = self.known_servers.get(successor_id)
            if not addr:
                logging.warning(
                    "%s: no address for successor %s", self.server_id, successor_id
                )
                continue

            try:
                async with grpc.aio.insecure_channel(addr) as ch:
                    stub = RingElectionStub(ch)
                    await asyncio.wait_for(stub.Election(msg), timeout=ELECTION_TIMEOUT)
                    logging.info(
                        "%s: forwarded Election to %s (%s)",
                        self.server_id,
                        successor_id,
                        addr,
                    )
                    return True
            except Exception as exc:
                logging.info(
                    "%s: forward_election to %s (%s) failed: %s",
                    self.server_id,
                    successor_id,
                    addr,
                    type(exc).__name__,
                )
                await asyncio.sleep(
                    random.uniform(PER_PEER_DELAY_MIN, PER_PEER_DELAY_MAX)
                )
                continue

        logging.warning(
            "%s: failed to forward election to any successor", self.server_id
        )
        return False

    async def _forward_leader(self, leader_msg: LeaderMessage) -> bool:
        """Forward leader message to next reachable successor in ring"""
        next_id = self._get_next_in_ring(self.server_id)

        # Try to forward to immediate successor first, then try others
        successors = [next_id] + [
            s for s in self._get_reachable_successors(self.server_id) if s != next_id
        ]

        for successor_id in successors:
            addr = self.known_servers.get(successor_id)
            if not addr:
                continue

            try:
                async with grpc.aio.insecure_channel(addr) as ch:
                    stub = RingElectionStub(ch)
                    await asyncio.wait_for(
                        stub.Leader(leader_msg), timeout=ELECTION_TIMEOUT
                    )
                    logging.info(
                        "%s: forwarded Leader to %s (%s)",
                        self.server_id,
                        successor_id,
                        addr,
                    )
                    return True
            except Exception as exc:
                logging.info(
                    "%s: forward_leader to %s (%s) failed: %s",
                    self.server_id,
                    successor_id,
                    addr,
                    type(exc).__name__,
                )
                await asyncio.sleep(
                    random.uniform(PER_PEER_DELAY_MIN, PER_PEER_DELAY_MAX)
                )
                continue

        logging.warning(
            "%s: failed to forward leader message to any successor", self.server_id
        )
        return False

    async def _broadcast_leader(self, leader_id: str, originator_id: str):
        """Broadcast leader message around the ring"""
        logging.info(
            "%s: broadcasting leader=%s around ring", self.server_id, leader_id
        )

        leader_msg = LeaderMessage(leader_id=leader_id, originator_id=originator_id)
        success = await self._forward_leader(leader_msg)

        if not success:
            logging.warning("%s: failed to initiate leader broadcast", self.server_id)

    # ---- External APIs -------------------------------------------------
    async def StartElection(self):
        """Initiate a new election from this node"""
        async with self._lock:
            if self._election_in_progress:
                logging.info(
                    "%s: election already in progress, skipping", self.server_id
                )
                return

            self._election_in_progress = True
            logging.info(
                "%s: starting election (priority=%s)", self.server_id, self.priority
            )

        try:
            msg = ElectionMessage(
                originator_id=self.server_id,
                node_ids=[self.server_id],
                priorities=[int(self.priority)],
            )

            forwarded = await self._forward_election(msg)
            if not forwarded:
                logging.info(
                    "%s: no reachable successors; electing self as leader",
                    self.server_id,
                )
                async with self._lock:
                    self.leader = self.server_id
                    self._election_in_progress = False
                await self._broadcast_leader(self.server_id, self.server_id)
        except Exception as exc:
            logging.exception("%s: error during StartElection", self.server_id)
            async with self._lock:
                self._election_in_progress = False

    async def _check_leader_health(self) -> bool:
        """Check if current leader is still alive"""
        if not self.leader:
            return False

        leader_addr = self.known_servers.get(self.leader)
        if not leader_addr:
            logging.info(
                "%s: leader %s not in known_servers", self.server_id, self.leader
            )
            return False

        try:
            async with grpc.aio.insecure_channel(leader_addr) as ch:
                stub = RingElectionStub(ch)
                resp = await asyncio.wait_for(
                    stub.Request(RequestMessage()), timeout=ELECTION_TIMEOUT
                )
                logging.debug(
                    "%s: leader %s healthy (priority=%s)",
                    self.server_id,
                    self.leader,
                    resp.priority,
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

    async def LifeCycle(self):
        """Main lifecycle loop - manages elections and leader monitoring"""
        logging.info("%s: starting LifeCycle loop", self.server_id)

        while True:
            try:
                # If no leader known, start election
                if not self.leader:
                    logging.info(
                        "%s: no leader known, starting election", self.server_id
                    )
                    await self.StartElection()
                    await asyncio.sleep(ELECTION_ROUND_INTERVAL)
                    continue

                # Check if we're in the middle of an election
                async with self._lock:
                    election_in_progress = self._election_in_progress

                if election_in_progress:
                    logging.debug("%s: election in progress, waiting", self.server_id)
                    await asyncio.sleep(ELECTION_ROUND_INTERVAL)
                    continue

                # Check leader health
                leader_healthy = await self._check_leader_health()
                if not leader_healthy:
                    logging.info(
                        "%s: leader %s unreachable, clearing and starting election",
                        self.server_id,
                        self.leader,
                    )
                    async with self._lock:
                        self.leader = None
                    await self.StartElection()
                    await asyncio.sleep(ELECTION_ROUND_INTERVAL)
                else:
                    # Leader is healthy, wait longer before next check
                    await asyncio.sleep(LEADER_CHECK_INTERVAL)

            except asyncio.CancelledError:
                logging.info("%s: LifeCycle cancelled", self.server_id)
                raise
            except Exception as exc:
                logging.exception(
                    "%s: LifeCycle error, retrying after delay", self.server_id
                )
                await asyncio.sleep(ELECTION_ROUND_INTERVAL)

    def get_current_leader(self) -> Optional[str]:
        """Get the current leader (thread-safe)"""
        return self.leader

    def is_leader(self) -> bool:
        """Check if this node is the current leader"""
        return self.leader == self.server_id
