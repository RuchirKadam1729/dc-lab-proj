import asyncio
import logging
import random
import grpc
from BullyElection_pb2 import (
    CandidateMessage,
    LeaderMessage,
    RequestMessage,
    ResponseMessage,
    ElectionMessage,
)
from BullyElection_pb2_grpc import (
    BullyElectionServicer,
    BullyElectionStub,
    add_BullyElectionServicer_to_server,
)


# tuning knobs
ELECTION_ROUND_INTERVAL = 2.0  # seconds between rounds
PER_PEER_DELAY_MIN = 0.05
PER_PEER_DELAY_MAX = 0.12
LEADER_ANNOUNCE_PAUSE = 1.0


class BullyElectionImpl(BullyElectionServicer):
    """
    Bully election implementation:
    - known_servers: dict[server_id -> grpc_addr]
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
        self.known_servers: dict[str, str] = dict(known_servers or {})
        self.leader: str | None = None
        # optional: address string for this node (host:port) so we can skip contacting self
        self.self_grpc_addr = self_grpc_addr
        logging.info(
            "BullyElectionImpl created: id=%s priority=%s known=%s self_addr=%s",
            self.id,
            self.priority,
            list(self.known_servers.keys()),
            self.self_grpc_addr,
        )

    # RPCs other nodes can call on us (simple responses)
    async def Election(self, request, context) -> CandidateMessage:
        # Not used in this minimal implementation but keep for completeness
        return CandidateMessage(priority=self.priority)

    async def Leader(self, request: LeaderMessage, context) -> None:
        # remote telling us who the leader is
        self.leader = request.leader_id
        logging.info("%s: Leader RPC received, leader=%s", self.id, self.leader)
        return None

    async def Request(self, request, context) -> ResponseMessage:
        # peer asking our priority / alive-ness
        return ResponseMessage(priority=self.priority)

    # --- Core bully logic ------------------------------------------------
    async def ElectionPhase(self) -> bool:
        """
        Return True if this node determines it should declare itself leader,
        i.e., no reachable peer has strictly greater priority.
        """

        async def ask_peer(addr: str) -> int | None:
            # returns peer priority or None if unreachable
            try:
                async with grpc.aio.insecure_channel(addr) as ch:
                    stub = BullyElectionStub(ch)
                    resp: ResponseMessage = await asyncio.wait_for(
                        stub.Request(RequestMessage()), timeout=1.5
                    )
                    return int(resp.priority)
            except Exception as exc:
                logging.info("%s: ask_peer(%s) exception: %s", self.id, addr, exc)
                return None

        logging.info("%s: starting ElectionPhase, checking peers", self.id)
        results = []
        for peer_id, addr in self.known_servers.items():
            # skip self if present in map (match by addr or id)
            if addr == self.self_grpc_addr or peer_id == self.id:
                continue
            peer_priority = await ask_peer(addr)
            logging.info(
                "%s: ask_peer(%s -> %s) returned %s",
                self.id,
                peer_id,
                addr,
                peer_priority,
            )
            # small delay so logs from different nodes are readable
            await asyncio.sleep(random.uniform(PER_PEER_DELAY_MIN, PER_PEER_DELAY_MAX))
            if peer_priority is None:
                # unreachable → treat as not strictly higher (so it doesn't block us)
                results.append(True)
            else:
                # True means peer is lower or equal priority (OK for us), False means peer is higher (we lose)
                results.append(peer_priority <= self.priority)
        # If all results are True => no reachable peer had priority > self → we can be leader
        am_leader = all(results) if results else True
        logging.info(
            "%s: ElectionPhase result -> %s (results=%s)", self.id, am_leader, results
        )
        return am_leader

    async def LeaderPhase(self) -> bool:
        """
        After ElectionPhase says we can be leader, broadcast Leader message to all peers.
        Return True if broadcast completed (best-effort).
        """
        logging.info(
            "%s: entering LeaderPhase - announcing leadership to peers", self.id
        )

        async def inform_peer(addr: str) -> bool:
            try:
                async with grpc.aio.insecure_channel(addr) as ch:
                    stub = BullyElectionStub(ch)
                    await asyncio.wait_for(
                        stub.Leader(LeaderMessage(leader_id=self.id)), timeout=1.5
                    )
                    return True
            except Exception as exc:
                logging.info("%s: inform_peer(%s) failed/unreachable: %s", self.id, addr, exc)
                return False

        results = []
        for peer_id, addr in self.known_servers.items():
            if addr == self.self_grpc_addr or peer_id == self.id:
                continue
            ok = await inform_peer(addr)
            results.append(ok)
            await asyncio.sleep(random.uniform(PER_PEER_DELAY_MIN, PER_PEER_DELAY_MAX))
        logging.info(
            "%s: LeaderPhase finished, informed %d peers",
            self.id,
            sum(1 for r in results if r),
        )
        # best-effort success
        return True

    async def RequestPhase(self):
        """
        Optionally poll the known leader for heartbeats or perform leader-specific tasks.
        Keep simple: try one quick ping to current leader if known.
        """
        if not self.leader:
            return
        leader_addr = self.known_servers.get(self.leader)
        if not leader_addr:
            return
        try:
            async with grpc.aio.insecure_channel(leader_addr) as ch:
                stub = BullyElectionStub(ch)
                await asyncio.wait_for(stub.Request(RequestMessage()), timeout=1.0)
                logging.info(
                    "%s: RequestPhase: leader %s responded", self.id, self.leader
                )
        except Exception:
            logging.info(
                "%s: RequestPhase: leader %s unreachable", self.id, self.leader
            )
            # if leader unreachable, clear and force next election
            self.leader = None

    async def LifeCycle(self):
        """
        Main loop: run election rounds periodically. If we win, announce leader.
        """
        while True:
            try:
                logging.info(
                    "%s: starting election round (priority=%s)", self.id, self.priority
                )
                can_be_leader = await self.ElectionPhase()
                if can_be_leader:
                    logging.info(
                        "%s: ElectionPhase succeeded — attempting LeaderPhase", self.id
                    )
                    success = await self.LeaderPhase()
                    if success:
                        self.leader = self.id
                        logging.info(
                            "%s: I am the new leader (priority=%s)",
                            self.id,
                            self.priority,
                        )
                        # pause a bit after announcing (avoid immediate re-election spam)
                        await asyncio.sleep(LEADER_ANNOUNCE_PAUSE)
                else:
                    logging.info(
                        "%s: not leader this round; sleeping before next check", self.id
                    )
                    # give leader a chance to announce, and don't hammer
                    await asyncio.sleep(ELECTION_ROUND_INTERVAL)
                    await self.RequestPhase()
            except asyncio.CancelledError:
                logging.info("%s: LifeCycle cancelled", self.id)
                raise
            except Exception as exc:
                logging.exception(
                    "%s: LifeCycle error (sleeping then retrying)",
                    self.id,
                    exc_info=exc,
                )
                await asyncio.sleep(ELECTION_ROUND_INTERVAL)
