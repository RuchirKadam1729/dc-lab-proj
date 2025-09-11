from protogens.ChatServer_pb2 import *
from protogens.ChatServer_pb2_grpc import *
import grpc
from pydantic import BaseModel

# if needed later lol
# import ipaddress

# class Address(BaseModel):
#     ipaddr : ipaddress
#     port : int

known_addresses: set[str] = ("localhost:9001", "localhost:9002", "localhost:9003")


class Client(BaseModel):
    id: str
    name: str = "Anonymous"


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
            tuple(getattr(other.val, "payload", ())),
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
        return heapq.heappop(self.buf)


import json


class ChatServer(ChatServerServicer):

    def __init__(self, address):
        self.client_list: set[str] = ()
        self.address: str
        self.msgBuffers: dict[str, MsgBuffer]  # map of client id to msgbuffer

    def Forward(self, request: ChatMessage, context):
        if request.recipient_id in self.client_list:
            # if connection active (logic ill put later)
            boole = 1
            if boole:
                event = json.dumps(
                    """{
                    invoke: receive}"""
                )
                # send thru WS connection
            else:
                self.msgBuffers[request.recipient_id].buffer_in(request)
            pass
        else:
            for address in known_addresses:
                with grpc.insecure_channel(address) as channel:
                    stub = ChatServerStub(channel)
                    resp: ChatServerResponse = stub.Forward(ChatMessage(request))
                    if resp.status_code == resp.OK:
                        return resp.OK
            return resp.ERR
