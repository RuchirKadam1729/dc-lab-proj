#!/usr/bin/env python3
# ring_election.py
import asyncio
import logging
import random
from typing import Dict, List, Optional

import grpc
from google.protobuf.empty_pb2 import Empty

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
ELECTION_TIMEOUT = 1.5
ELECTION_RETRY_DELAY = 0.05
ELECTION_ROUND_INTERVAL = 2.0  # when leader up, how often to check
PER_PEER_DELAY_MIN = 0.02
PER_PEER_DELAY_MAX = 0.06


class RingElectionImpl(RingElectionServicer):
    """
    Ring-based election algorithm implementation.

    - known_servers: dict[server_id -> grpc_addr] representing the ring order (insertion order)
    - priority: numeric (higher -> wins)
    """

    def __init__(
        self,
        id: str,
        priority: int,
        leader_addr: str,
        known_servers: dict | None = None,
        self_grpc_addr: str | None = None,
    ):
        self.id = id
        self.priority = priority
        self.leader_addr = leader_addr
        self.known_servers: Dict[str, str] = dict(known_servers or {})
        self.self_grpc_addr = self_grpc_addr = self_grpc_addr
        self.leader: Optional[str] = None
        self._lock = asyncio.Lock()

        logging.info(
            "RingElectionImpl created: id=%s priority=%s known=%s self_addr=%s",
            self.id,
            self.priority,
            list(self.known_servers.keys()),
            self.self_grpc_addr,
        )

    # ---- RPC handlers ---------------------------------------------------
    async def Election(self, request: ElectionMessage, context) -> Empty:
        """
        Handle incoming election message:
        - if it's our originator's message returned to us -> decide winner & broadcast leader
        - otherwise append self and forward to next active member
        """
        logging.info(
            "%s: Election RPC received (originator=%s) nodes=%s priorities=%s",
            self.id,
            request.originator_id,
            list(request.node_ids),
            list(request.priorities),
        )

        # If the election message has returned to originator (someone's circulation completed)
        if request.originator_id == self.id:
            # choose highest priority in the returned list
            if not request.priorities or not request.node_ids:
                logging.warning(
                    "%s: Election message returned empty - electing self", self.id
                )
                leader_id = self.id
            else:
                # pick index of max priority (first occurrence)
                max_idx = max(
                    range(len(request.priorities)), key=lambda i: request.priorities[i]
                )
                leader_id = request.node_ids[max_idx]
            logging.info("%s: Election completed, chosen leader=%s", self.id, leader_id)

            # set local leader and broadcast coordinator around the ring
            self.leader = leader_id
            await self._broadcast_leader(leader_id, request.originator_id)
            return Empty()

        # not originator: append our info and forward
        # NOTE: request is a protobuf message; build a new message to send forward
        new_msg = ElectionMessage(
            originator_id=request.originator_id,
            node_ids=list(request.node_ids) + [self.id],
            priorities=list(request.priorities) + [int(self.priority)],
        )
        forwarded = await self._forward_election(new_msg)
        if not forwarded:
            # if we could not forward to anyone (we're alone/almost alone), elect self and broadcast
            logging.info("%s: could not forward election; electing self", self.id)
            self.leader = self.id
            await self._broadcast_leader(self.id, request.originator_id)
        return Empty()

    async def Leader(self, request: LeaderMessage, context) -> Empty:
        """
        Coordinator announcement. Set local leader and forward around ring until originator seen.
        """
        logging.info(
            "%s: Leader RPC received leader=%s originator=%s",
            self.id,
            request.leader_id,
            request.originator_id,
        )
        self.leader = request.leader_id

        # If the originator is us, the message completed its round — we're done.
        if request.originator_id == self.id:
            logging.info(
                "%s: Leader message returned to originator, stopping propagation",
                self.id,
            )
            return Empty()

        # Forward leader announcement to next active member
        await self._forward_leader(request)
        return Empty()

    async def Request(self, request: RequestMessage, context) -> ResponseMessage:
        """Simple ping: return our priority so others can evaluate liveness + priority."""
        logging.debug("%s: Request RPC received (ping)", self.id)
        return ResponseMessage(priority=int(self.priority))

    # ---- Helpers --------------------------------------------------------

    def _ring_order(self) -> List[str]:
        """Return node ids in ring order (insertion order of known_servers). If self not present, include it."""
        keys = list(self.known_servers.keys())
        if self.id not in keys:
            # place self at end to avoid changing the client-specified ordering
            keys.append(self.id)
        return keys

    def _index_in_ring(self, node_id: str) -> int:
        keys = self._ring_order()
        try:
            return keys.index(node_id)
        except ValueError:
            return -1

    async def _forward_election(self, msg: ElectionMessage) -> bool:
        """
        Try to forward the election message to the next reachable successor.
        Return True on success, False if no successor reachable.
        """
        keys = self._ring_order()
        if self.id not in keys:
            keys.append(self.id)
        n = len(keys)
        if n <= 1:
            return False

        start_idx = self._index_in_ring(self.id)
        assert start_idx >= 0

        # iterate successors in ring order
        for offset in range(1, n):
            idx = (start_idx + offset) % n
            peer_id = keys[idx]
            if peer_id == self.id:
                continue
            addr = self.known_servers.get(peer_id)
            if not addr:
                continue
            try:
                async with grpc.aio.insecure_channel(addr) as ch:
                    stub = RingElectionStub(ch)
                    await asyncio.wait_for(stub.Election(msg), timeout=ELECTION_TIMEOUT)
                    logging.info(
                        "%s: forwarded Election to %s (%s)", self.id, peer_id, addr
                    )
                    return True
            except Exception as exc:
                logging.info(
                    "%s: forward_election to %s (%s) failed: %s",
                    self.id,
                    peer_id,
                    addr,
                    exc,
                )
                # try next successor
                await asyncio.sleep(
                    random.uniform(PER_PEER_DELAY_MIN, PER_PEER_DELAY_MAX)
                )
                continue
        return False

    async def _forward_leader(self, leader_msg: LeaderMessage) -> bool:
        """
        Forward a LeaderMessage to the next reachable successor (best-effort).
        Return True on success, False otherwise.
        """
        keys = self._ring_order()
        if self.id not in keys:
            keys.append(self.id)
        n = len(keys)
        if n <= 1:
            return False

        start_idx = self._index_in_ring(self.id)
        for offset in range(1, n):
            idx = (start_idx + offset) % n
            peer_id = keys[idx]
            if peer_id == self.id:
                continue
            addr = self.known_servers.get(peer_id)
            if not addr:
                continue
            try:
                async with grpc.aio.insecure_channel(addr) as ch:
                    stub = RingElectionStub(ch)
                    await asyncio.wait_for(
                        stub.Leader(leader_msg), timeout=ELECTION_TIMEOUT
                    )
                    logging.info(
                        "%s: forwarded Leader to %s (%s)", self.id, peer_id, addr
                    )
                    return True
            except Exception as exc:
                logging.info(
                    "%s: forward_leader to %s (%s) failed: %s",
                    self.id,
                    peer_id,
                    addr,
                    exc,
                )
                await asyncio.sleep(
                    random.uniform(PER_PEER_DELAY_MIN, PER_PEER_DELAY_MAX)
                )
                continue
        return False

    async def _broadcast_leader(self, leader_id: str, originator_id: str):
        """
        Broadcast coordinator announcement in a best-effort fashion.
        We attempt to call Leader RPC on successors until done.
        To keep it simple and robust we do per-peer informs (like bully did).
        """
        logging.info("%s: broadcasting leader=%s to peers", self.id, leader_id)
        results = []
        for peer_id, addr in self.known_servers.items():
            if peer_id == self.id:
                continue
            try:
                async with grpc.aio.insecure_channel(addr) as ch:
                    stub = RingElectionStub(ch)
                    await asyncio.wait_for(
                        stub.Leader(
                            LeaderMessage(
                                leader_id=leader_id, originator_id=originator_id
                            )
                        ),
                        timeout=ELECTION_TIMEOUT,
                    )
                    results.append(True)
            except Exception as exc:
                logging.info("%s: inform_peer(%s) failed: %s", self.id, peer_id, exc)
                results.append(False)
                await asyncio.sleep(
                    random.uniform(PER_PEER_DELAY_MIN, PER_PEER_DELAY_MAX)
                )
        logging.info(
            "%s: broadcast completed, informed %d peers",
            self.id,
            sum(1 for r in results if r),
        )

    # ---- External APIs -------------------------------------------------

    async def StartElection(self):
        """
        Initiate an election: create election message and send it to the first reachable successor.
        """
        async with self._lock:
            logging.info(
                "%s: starting local StartElection (priority=%s)", self.id, self.priority
            )
            msg = ElectionMessage(
                originator_id=self.id,
                node_ids=[self.id],
                priorities=[int(self.priority)],
            )
            forwarded = await self._forward_election(msg)
            if not forwarded:
                # couldn't forward -> we are alone (or no reachable peer) -> become leader and broadcast
                logging.info(
                    "%s: StartElection: no reachable successor; electing self", self.id
                )
                self.leader = self.id
                await self._broadcast_leader(self.id, self.id)

    async def LifeCycle(self):
        """
        Periodic loop: ping current leader; if unreachable, start election.
        """
        while True:
            try:
                if not self.leader:
                    # no known leader → start election
                    logging.info("%s: no leader known, starting election", self.id)
                    await self.StartElection()
                    await asyncio.sleep(ELECTION_ROUND_INTERVAL)
                    continue

                leader_addr = self.known_servers.get(self.leader)
                if not leader_addr:
                    logging.info(
                        "%s: known leader not in known_servers; clearing leader",
                        self.id,
                    )
                    self.leader = None
                    await asyncio.sleep(ELECTION_ROUND_INTERVAL)
                    continue

                try:
                    async with grpc.aio.insecure_channel(leader_addr) as ch:
                        stub = RingElectionStub(ch)
                        resp = await asyncio.wait_for(
                            stub.Request(RequestMessage()), timeout=ELECTION_TIMEOUT
                        )
                        logging.debug(
                            "%s: leader %s responded (priority=%s)",
                            self.id,
                            self.leader,
                            resp.priority,
                        )
                except Exception:
                    logging.info(
                        "%s: leader %s unreachable -> clearing leader and starting election",
                        self.id,
                        self.leader,
                    )
                    self.leader = None
                    await self.StartElection()

                await asyncio.sleep(ELECTION_ROUND_INTERVAL)
            except asyncio.CancelledError:
                logging.info("%s: LifeCycle cancelled", self.id)
                raise
            except Exception as exc:
                logging.exception(
                    "%s: LifeCycle error (sleeping then retrying)", self.id, exc
                )
                await asyncio.sleep(ELECTION_ROUND_INTERVAL)
