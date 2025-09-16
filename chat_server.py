from protos.ChatServer_pb2 import ChatMessage, ChatServerResponse
from protos.ChatServer_pb2_grpc import ChatServerServicer, ChatServerStub

import grpc

known_servers_grpc = dict(
    {"0.0.0.0:9001": "srv-A", "0.0.0.0:9002": "srv-B", "0.0.0.0:9003": "srv-C"}
)
own_servers_ws = dict(
    {
        "ws://0.0.0.0:19001": "srv-A",
        "ws://0.0.0.0:19002": "srv-B",
        "ws://0.0.0.0:19003": "srv-C",
    }
)
known_seq_nums = dict({"0.0.0.0:9001": 1, "0.0.0.0:9002": 2, "0.0.0.0:9003": 3})
known_priors = dict({"0.0.0.0:9001": 3, "0.0.0.0:9002": 2, "0.0.0.0:9003": 1})


class MsgBufferNode:
    def __init__(self, val: ChatMessage):
        self.val = val

    @staticmethod  # below is gpt-generated vclock ordering based on logic i told it
    def _vc_cmp(a: dict, b: dict) -> int:
        """Return -1 if a < b, 1 if a > b, 0 if concurrent/equal."""
        keys = sorted(set(a.keys()) | set(b.keys()))
        pairs = ((a.get(k, 0), b.get(k, 0)) for k in keys)
        a_le_b = all(x <= y for x, y in pairs)

        pairs = ((a.get(k, 0), b.get(k, 0)) for k in keys)
        b_le_a = all(x >= y for x, y in pairs)

        pairs = ((a.get(k, 0), b.get(k, 0)) for k in keys)
        a_lt_b = any(x < y for x, y in pairs)
        pairs = ((a.get(k, 0), b.get(k, 0)) for k in keys)
        b_lt_a = any(x > y for x, y in pairs)

        if a_le_b and a_lt_b:
            return -1
        if b_le_a and b_lt_a:
            return 1
        return 0

    # me actually implementing said logic
    def __lt__(self, other: "MsgBufferNode") -> bool:
        a_vc = dict(getattr(self.val, "v_clock", {}) or {})
        b_vc = dict(getattr(other.val, "v_clock", {}) or {})
        cmp = self._vc_cmp(a_vc, b_vc)
        if cmp != 0:
            return cmp == -1

        # tie-breaker for equal clocks
        a_key = (
            getattr(self.val, "sender_id", ""),
            getattr(self.val, "recipient_id", ""),
            tuple(getattr(self.val, "payload", ())),
        )
        b_key = (
            getattr(other.val, "sender_id", ""),
            getattr(other.val, "recipient_id", ""),
        )
        return a_key < b_key


from dataclasses import dataclass
import heapq


@dataclass
class MsgBuffer:
    buf: list[MsgBufferNode]

    def buffer_in(self, msg: ChatMessage):
        heapq.heappush(self.buf, MsgBufferNode(msg))

    def buffer_out(self) -> ChatMessage:
        return heapq.heappop(self.buf).val


import json
from SenderAsync import SenderAsync

from datetime import datetime
from random import randint
from bully_election import BullyElectionImpl


class ChatServer(ChatServerServicer):

    def __init__(self, address, known_servers=[]):
        self.id: str
        self.msgBuffers: dict[str, MsgBuffer]
        self.clients: dict[str, SenderAsync]
        self.seen_msg_ids = list()
        self.known_servers = known_servers
        self.address = address
        self.v_clock: dict[str, int] = {}
        self.date_time: datetime = datetime.now()
        self.bully_election_impl: BullyElectionImpl = BullyElectionImpl(
            id=self.id, priority=randint(1, 9), known_servers=known_servers
        )

    async def Forward(self, request: ChatMessage, context=None):
        for key, value in request.v_clock.items():
            self.v_clock[key] = max(self.v_clock.get(key, 0), value)
        for key in request.v_clock.keys():
            request.v_clock[key] = self.v_clock[key]

        # might still be buggy :/
        if request.msg_id in self.seen_msg_ids:
            return "DUP"
        self.seen_msg_ids.append(request.msg_id)

        sender = self.clients.get(request.recipient_id)
        if sender:
            sender.send(request.SerializeToString())
            return "DELIVERED_LOCAL"

        if request.recipient_id in self.clients:
            self.msgBuffers[request.recipient_id].buffer_in(request)
            return "QUEUED_LOCAL"

        for addr in self.known_servers:
            try:
                async with grpc.aio.insecure_channel(addr) as ch:
                    stub = ChatServerStub(ch)
                    resp: ChatServerResponse = await stub.Forward(request)
                    if resp.status_code == resp.OK:
                        return "DELIVERED_REMOTE"
                    if resp.status_code == resp.ERR:
                        return "QUEUED_REMOTE"
            except Exception:
                continue
        self.msgBuffers[request.recipient_id].buffer_in(request)
        return "QUEUED_FALLBACK"

    async def handler(self, websocket):
        while 1:
            pass


import asyncio, websockets
from websockets.asyncio.server import serve


async def main():
    import sys

    cs = ChatServer("0.0.0.0:" + sys.argv[1])

    asyncio.run(cs.bully_election_impl.LifeCycle())
    async with serve(cs.handler, "0.0.0.0", int(sys.argv[1])) as ws:
        await ws.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
