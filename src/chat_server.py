from ChatServer_pb2 import ChatMessage, ChatServerResponse
from ChatServer_pb2_grpc import ChatServerServicer, ChatServerStub

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


from dataclasses import dataclass
import heapq
from MsgBufferNode import MsgBufferNode


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
from random import randint, randbytes
from bully_election import BullyElectionImpl


class ChatServer(ChatServerServicer):

    def __init__(self, address, known_servers=[]):
        self.id: str = str(randbytes(14))
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

    async def Forward(self, request: ChatMessage, context=None) -> ChatServerResponse:
        for key, value in request.v_clock.items():
            self.v_clock[key] = max(self.v_clock.get(key, 0), value)
        for key in request.v_clock.keys():
            request.v_clock[key] = self.v_clock[key]

        # might still be buggy :/
        if request.msg_id in self.seen_msg_ids:
            ChatServerResponse(status_code=ChatServerResponse.DUP)
        self.seen_msg_ids.append(request.msg_id)

        sender = self.clients.get(request.recipient_id)
        if sender:
            sender.send(request.SerializeToString())
            return ChatServerResponse(status_code=ChatServerResponse.DELIVERED_LOCAL)

        if request.recipient_id in self.clients:
            self.msgBuffers[request.recipient_id].buffer_in(request)
            return ChatServerResponse(status_code=ChatServerResponse.QUEUED_LOCAL)

        for addr in self.known_servers:
            try:
                async with grpc.aio.insecure_channel(addr) as ch:
                    stub = ChatServerStub(ch)
                    resp: ChatServerResponse = await stub.Forward(request)
                    if resp.status_code == resp.DELIVERED_LOCAL | resp.DELIVERED_REMOTE:
                        return ChatServerResponse(
                            status_code=ChatServerResponse.DELIVERED_REMOTE
                        )
                    else:
                        return ChatServerResponse(
                            status_code=ChatServerResponse.QUEUED_REMOTE
                        )
            except Exception:
                continue
        self.msgBuffers[request.recipient_id].buffer_in(request)
        return ChatServerResponse(status_code=ChatServerResponse.QUEUED_FALLBACK)

    async def handler(self, websocket):
        async for message in websocket:
            print("Received message:", message)
            await self.Forward(message)


import asyncio, websockets
from websockets.asyncio.server import serve


async def main():
    import sys

    cs = ChatServer("0.0.0.0:" + sys.argv[1])

    asyncio.create_task(cs.bully_election_impl.LifeCycle())
    async with serve(cs.handler, "0.0.0.0", int(sys.argv[1])) as ws:
        await ws.serve_forever()


if __name__ == "__main__":
    import logging

    logging.basicConfig()
    asyncio.run(main())
