#!/usr/bin/env python3
# chat_server.py
import asyncio
import json
import logging
import heapq
from dataclasses import dataclass
from datetime import datetime
from random import randbytes
from typing import List

import grpc
from google.protobuf.json_format import MessageToJson, Parse
from google.protobuf.empty_pb2 import Empty
from websockets.asyncio.server import serve

from ChatServer_pb2 import ChatMessage, ChatServerResponse
from ChatServer_pb2_grpc import (
    ChatServerServicer,
    ChatServerStub,
    add_ChatServerServicer_to_server,
)
from BullyElection_pb2_grpc import add_BullyElectionServicer_to_server

# local modules - ensure these exist in your project
from MsgBufferNode import MsgBufferNode
from SenderAsync import SenderAsync
from bully_election import BullyElectionImpl

logging.basicConfig(level=logging.INFO)


@dataclass
class MsgBuffer:
    buf: List[MsgBufferNode]

    def buffer_in(self, msg: ChatMessage):
        heapq.heappush(self.buf, MsgBufferNode(msg))

    def buffer_out(self) -> ChatMessage:
        return heapq.heappop(self.buf).val


default_leader_addr = "0.0.0.0:12345"


class ChatServer(ChatServerServicer):
    def __init__(
        self,
        ws_addr: str,
        grpc_addr: str,
        priority: int,
        seq_num: int,
        known_ws_servers: dict | None = None,
        known_grpc_servers: dict | None = None,
    ):
        from collections import defaultdict

        # printable hex id
        self.id = randbytes(14).hex()
        # create buffers on first access to avoid KeyError
        self.msgBuffers: dict[str, MsgBuffer] = defaultdict(lambda: MsgBuffer([]))
        self.clients: dict[str, SenderAsync] = {}
        # set for O(1) membership checks; replace later with an LRU if needed
        self.seen_msg_ids = set()
        self.known_ws_servers = dict(known_ws_servers or {})
        self.known_grpc_servers = dict(known_grpc_servers or {})
        self.ws_addr = ws_addr
        self.grpc_addr = grpc_addr
        self.priority = priority
        self.seq_num = seq_num
        self.v_clock: dict[str, int] = {}
        self.date_time: datetime = datetime.now()
        self.bully_election_impl: BullyElectionImpl = BullyElectionImpl(
            id=self.id,
            priority=self.priority,
            known_servers=self.known_grpc_servers,
            leader_addr=default_leader_addr,
            self_grpc_addr=self.grpc_addr,
        )

    # ChatServer gRPC method
    async def Forward(self, request: ChatMessage, context=None) -> ChatServerResponse:
        # Merge incoming v-clock into server clock
        for k, v in request.v_clock.items():
            self.v_clock[k] = max(self.v_clock.get(k, 0), v)

        # Record that this server observed the event
        self.v_clock[self.id] = self.v_clock.get(self.id, 0) + 1

        # Replace the message's v_clock with the merged/latest one
        request.v_clock.clear()
        request.v_clock.update(self.v_clock)

        # dedupe: return immediately on duplicate
        if request.msg_id in self.seen_msg_ids:
            return ChatServerResponse(status_code=ChatServerResponse.DUP)
        self.seen_msg_ids.add(request.msg_id)

        # send if recipient connected, otherwise queue locally
        sender = self.clients.get(request.recipient_id)
        if sender:
            # fire-and-forget enqueue. sender.send is non-awaitable by design.
            sender.send(MessageToJson(request))
            return ChatServerResponse(status_code=ChatServerResponse.DELIVERED_LOCAL)

        # recipient not connected -> queue locally
        self.msgBuffers[request.recipient_id].buffer_in(request)
        return ChatServerResponse(status_code=ChatServerResponse.QUEUED_LOCAL)

    async def handler(self, websocket):
        # 1) expect registration JSON as first message
        reg_raw = await websocket.recv()
        reg = json.loads(reg_raw)
        client_id = reg.get("client_id")
        if not client_id:
            await websocket.close()
            return

        # 2) create sender and register
        sender = SenderAsync(websocket)
        self.clients[client_id] = sender
        await sender.start()

        # 3) drain any queued messages for this client (swap & send)
        buf = self.msgBuffers.pop(client_id, None)
        if buf:
            while buf.buf:
                m = buf.buffer_out()
                # send JSON (your client expects JSON), or use SerializeToString if using binary
                sender.send(MessageToJson(m))

        # 4) reader loop: parse incoming messages and forward them
        try:
            async for message_json in websocket:
                message = Parse(message_json, ChatMessage())
                logging.info("Received message from client %s: %s", client_id, message_json)
                await self.Forward(message)
        finally:
            # cleanup
            self.clients.pop(client_id, None)
            await sender.close()


async def main():
    import sys

    args: dict[str, str] = {}

    # simple arg parsing (expects pairs)
    for i in range(1, len(sys.argv) - 1):
        arg = sys.argv[i]
        next_arg = sys.argv[i + 1]
        match arg:
            case "--ws_port":
                args["ws_port"] = next_arg
            case "--grpc_port":
                args["grpc_port"] = next_arg
            case "--priority":
                args["priority"] = next_arg
            case "--seqnum":
                args["seqnum"] = next_arg

    KNOWN_WS = {
        "srv-A": "0.0.0.0:9001",
        "srv-B": "0.0.0.0:9002",
        "srv-C": "0.0.0.0:9003",
    }
    KNOWN_GRPC = {
        "srv-A": "127.0.0.1:50051",
        "srv-B": "127.0.0.1:50052",
        "srv-C": "127.0.0.1:50053",
    }

    cs = ChatServer(
        ws_addr=f'ws://0.0.0.0:{args["ws_port"]}',
        grpc_addr=f'127.0.0.1:{args["grpc_port"]}',
        priority=int(args["priority"]),
        seq_num=int(args["seqnum"]),
        known_ws_servers=KNOWN_WS,
        known_grpc_servers=KNOWN_GRPC,
    )

    # start gRPC aio server so peers can contact us
    grpc_server = grpc.aio.server()
    # register your servicer implementations
    add_ChatServerServicer_to_server(cs, grpc_server)
    # register bully election servicer so peers respond to election RPCs
    add_BullyElectionServicer_to_server(cs.bully_election_impl, grpc_server)

    # bind wildcard so kernel accepts incoming wire connections
    bind_port = cs.grpc_addr.split(":", 1)[1]  # e.g. "50051"
    grpc_server.add_insecure_port(f"0.0.0.0:{bind_port}")
    await grpc_server.start()
    logging.info("gRPC aio server listening (bound 0.0.0.0:%s) advertising %s", bind_port, cs.grpc_addr)

    # keep it running in background (only schedule once)
    asyncio.create_task(grpc_server.wait_for_termination())

    # start bully lifecycle (background task) and make failures visible
    life_task = asyncio.create_task(cs.bully_election_impl.LifeCycle())

    def _on_done(t: asyncio.Task):
        try:
            exc = t.exception()
            if exc:
                logging.exception("Bully LifeCycle task failed", exc_info=exc)
            else:
                logging.info("Bully LifeCycle task finished normally")
        except asyncio.CancelledError:
            logging.info("Bully LifeCycle cancelled")

    life_task.add_done_callback(_on_done)
    logging.info("started Bully LifeCycle task %s", life_task)

    # run one immediate election round so the node acts right away (handy for testing)
    asyncio.create_task(cs.bully_election_impl.ElectionPhase())

    async with serve(cs.handler, "0.0.0.0", int(args["ws_port"])) as ws:
        print(
            f"server started at {cs.ws_addr}, grpc {cs.grpc_addr}, priority {cs.priority}, seqnum: {cs.seq_num}"
        )
        await ws.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
