from protogens.ChatServer_pb2 import *
from protogens.ChatServer_pb2_grpc import * # type: ignore
import grpc

known_servers = dict(
    {"0.0.0.0:9001": "srv-A", "0.0.0.0:9002": "srv-B", "0.0.0.0:9003": "srv-C"}
)


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


class ChatServer(ChatServerServicer):

    def __init__(self, address, known_servers=[]):
        self.msgBuffers: dict[str, MsgBuffer]
        self.clients: dict[str, SenderAsync]
        self.seen_msg_ids = list()
        self.known_servers = known_servers
        self.address = address
        self.v_clock: dict[str, int] = {}
        self.date_time: datetime = datetime.now()

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
