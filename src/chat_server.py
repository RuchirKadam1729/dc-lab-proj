#!/usr/bin/env python3
# chat_server.py
import asyncio
import json
import logging
import heapq
from dataclasses import dataclass
from datetime import datetime
from random import randbytes
from typing import List, Dict, Optional

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

# Ring election servicer registration
from RingElection_pb2_grpc import add_RingElectionServicer_to_server

# local modules - ensure these exist in your project
from MsgBufferNode import MsgBufferNode
from SenderAsync import SenderAsync
from ring_election import RingElectionImpl

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


@dataclass
class MsgBuffer:
    buf: List[MsgBufferNode]

    def buffer_in(self, msg: ChatMessage):
        heapq.heappush(self.buf, MsgBufferNode(msg))

    def buffer_out(self) -> ChatMessage:
        return heapq.heappop(self.buf).val


# Define the ring topology - this determines the election order
RING_TOPOLOGY = [
    ("srv-A", "127.0.0.1:50051", "0.0.0.0:9001"),
    ("srv-B", "127.0.0.1:50052", "0.0.0.0:9002"),
    ("srv-C", "127.0.0.1:50053", "0.0.0.0:9003"),
]

# Create ordered dictionaries preserving ring order
from collections import OrderedDict

KNOWN_GRPC = OrderedDict(
    [(srv_id, grpc_addr) for srv_id, grpc_addr, _ in RING_TOPOLOGY]
)
KNOWN_WS = OrderedDict([(srv_id, ws_addr) for srv_id, _, ws_addr in RING_TOPOLOGY])

# Create port mappings for server identification
PORT_TO_SERVER_ID = {
    "50051": "srv-A",
    "50052": "srv-B",
    "50053": "srv-C",
}

default_leader_addr = "127.0.0.1:50051"  # srv-A's address


class ChatServer(ChatServerServicer):
    def __init__(
        self,
        server_id: str,
        ws_addr: str,
        grpc_addr: str,
        priority: int,
        seq_num: int,
        known_ws_servers: Dict[str, str] | None = None,
        known_grpc_servers: Dict[str, str] | None = None,
    ):
        from collections import defaultdict, OrderedDict

        self.server_id = server_id  # logical server ID (srv-A, srv-B, etc.)
        # printable hex id for unique message identification
        self.unique_id = randbytes(8).hex()

        # keep insertion-order for ring; use OrderedDict to guarantee order
        self.known_ws_servers = OrderedDict(known_ws_servers or {})
        self.known_grpc_servers = OrderedDict(known_grpc_servers or {})

        # create buffers on first access to avoid KeyError
        self.msgBuffers: dict[str, MsgBuffer] = defaultdict(lambda: MsgBuffer([]))
        self.clients: dict[str, SenderAsync] = {}
        # set for O(1) membership checks
        self.seen_msg_ids = set()

        self.ws_addr = ws_addr
        self.grpc_addr = grpc_addr
        self.priority = priority
        self.seq_num = seq_num
        self.v_clock: dict[str, int] = {}
        self.date_time: datetime = datetime.now()

        logging.info(
            "ChatServer created: server_id=%s, unique_id=%s, priority=%s, grpc_addr=%s",
            self.server_id,
            self.unique_id,
            self.priority,
            self.grpc_addr,
        )

        # attach ring election impl as attribute
        self.ring_election_impl: RingElectionImpl = RingElectionImpl(
            server_id=self.server_id,  # Use logical server ID
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
        self.v_clock[self.server_id] = self.v_clock.get(self.server_id, 0) + 1

        # Replace the message's v_clock with the merged/latest one
        request.v_clock.clear()
        request.v_clock.update(self.v_clock)

        # dedupe: return immediately on duplicate
        if request.msg_id in self.seen_msg_ids:
            logging.debug("%s: duplicate message %s", self.server_id, request.msg_id)
            return ChatServerResponse(status_code=ChatServerResponse.DUP)
        self.seen_msg_ids.add(request.msg_id)

        # send if recipient connected, otherwise queue locally
        sender = self.clients.get(request.recipient_id)
        if sender:
            # fire-and-forget enqueue. sender.send is non-awaitable by design.
            sender.send(MessageToJson(request))
            logging.info(
                "%s: delivered message %s to local client %s",
                self.server_id,
                request.msg_id,
                request.recipient_id,
            )
            return ChatServerResponse(status_code=ChatServerResponse.DELIVERED_LOCAL)

        # recipient not connected -> queue locally
        self.msgBuffers[request.recipient_id].buffer_in(request)
        logging.info(
            "%s: queued message %s for client %s",
            self.server_id,
            request.msg_id,
            request.recipient_id,
        )
        return ChatServerResponse(status_code=ChatServerResponse.QUEUED_LOCAL)

    async def handler(self, websocket):
        client_id = None
        try:
            # 1) expect registration JSON as first message
            reg_raw = await websocket.recv()
            reg = json.loads(reg_raw)
            client_id = reg.get("client_id")
            if not client_id:
                logging.warning(
                    "%s: websocket connection without client_id", self.server_id
                )
                await websocket.close()
                return

            logging.info(
                "%s: client %s connected via websocket", self.server_id, client_id
            )

            # 2) create sender and register
            sender = SenderAsync(websocket)
            self.clients[client_id] = sender
            await sender.start()

            # 3) drain any queued messages for this client (swap & send)
            buf = self.msgBuffers.pop(client_id, None)
            if buf:
                message_count = len(buf.buf)
                logging.info(
                    "%s: draining %d queued messages for client %s",
                    self.server_id,
                    message_count,
                    client_id,
                )
                while buf.buf:
                    m = buf.buffer_out()
                    # send JSON (your client expects JSON)
                    sender.send(MessageToJson(m))

            # 4) reader loop: parse incoming messages and forward them
            async for message_json in websocket:
                try:
                    message = Parse(message_json, ChatMessage())
                    logging.info(
                        "%s: received message from client %s to %s",
                        self.server_id,
                        client_id,
                        message.recipient_id,
                    )
                    await self.Forward(message)
                except Exception as e:
                    logging.error(
                        "%s: error processing message from client %s: %s",
                        self.server_id,
                        client_id,
                        e,
                    )

        except Exception as e:
            logging.error(
                "%s: websocket handler error for client %s: %s",
                self.server_id,
                client_id,
                e,
            )
        finally:
            # cleanup
            if client_id:
                self.clients.pop(client_id, None)
                logging.info("%s: client %s disconnected", self.server_id, client_id)
            if "sender" in locals():
                await sender.close()

    def get_status(self) -> dict:
        """Get server status including election info"""
        return {
            "server_id": self.server_id,
            "unique_id": self.unique_id,
            "priority": self.priority,
            "grpc_addr": self.grpc_addr,
            "ws_addr": self.ws_addr,
            "current_leader": self.ring_election_impl.get_current_leader(),
            "is_leader": self.ring_election_impl.is_leader(),
            "connected_clients": list(self.clients.keys()),
            "queued_messages": {
                client_id: len(buf.buf) for client_id, buf in self.msgBuffers.items()
            },
        }


async def setup_server(args: dict) -> ChatServer:
    """Setup server with proper identification"""
    grpc_port = args["grpc_port"]

    # Identify which server this is based on port
    if grpc_port not in PORT_TO_SERVER_ID:
        raise ValueError(
            f"Unknown gRPC port {grpc_port}. Must be one of: {list(PORT_TO_SERVER_ID.keys())}"
        )

    server_id = PORT_TO_SERVER_ID[grpc_port]

    # Get addresses from topology
    grpc_addr = KNOWN_GRPC[server_id]
    ws_addr = KNOWN_WS[server_id]

    logging.info(
        "Setting up server: id=%s, grpc=%s, ws=%s", server_id, grpc_addr, ws_addr
    )

    cs = ChatServer(
        server_id=server_id,
        ws_addr=ws_addr,
        grpc_addr=grpc_addr,
        priority=int(args["priority"]),
        seq_num=int(args["seqnum"]),
        known_ws_servers=KNOWN_WS,
        known_grpc_servers=KNOWN_GRPC,
    )

    return cs


async def start_grpc_server(cs: ChatServer) -> grpc.aio.Server:
    """Start gRPC server for inter-node communication"""
    grpc_server = grpc.aio.server()

    # register servicers
    add_ChatServerServicer_to_server(cs, grpc_server)
    add_RingElectionServicer_to_server(cs.ring_election_impl, grpc_server)

    # bind wildcard so kernel accepts incoming wire connections
    bind_port = cs.grpc_addr.split(":", 1)[1]  # e.g. "50051"
    grpc_server.add_insecure_port(f"0.0.0.0:{bind_port}")

    await grpc_server.start()
    logging.info(
        "%s: gRPC server listening on 0.0.0.0:%s (advertising %s)",
        cs.server_id,
        bind_port,
        cs.grpc_addr,
    )

    return grpc_server


async def start_election_lifecycle(cs: ChatServer) -> asyncio.Task:
    """Start the ring election lifecycle"""

    def _on_lifecycle_done(task: asyncio.Task):
        try:
            exc = task.exception()
            if exc:
                logging.exception(
                    "%s: Ring LifeCycle task failed", cs.server_id, exc_info=exc
                )
            else:
                logging.info("%s: Ring LifeCycle task finished normally", cs.server_id)
        except asyncio.CancelledError:
            logging.info("%s: Ring LifeCycle cancelled", cs.server_id)

    # Start background lifecycle task
    lifecycle_task = asyncio.create_task(cs.ring_election_impl.LifeCycle())
    lifecycle_task.add_done_callback(_on_lifecycle_done)

    logging.info("%s: started Ring LifeCycle task", cs.server_id)

    # Run one immediate election round for faster startup
    asyncio.create_task(cs.ring_election_impl.StartElection())

    return lifecycle_task


async def main():
    import sys

    # Parse command line arguments
    args: dict[str, str] = {}
    i = 1
    while i < len(sys.argv) - 1:
        arg = sys.argv[i]
        next_arg = sys.argv[i + 1]
        if arg in ["--ws_port", "--grpc_port", "--priority", "--seqnum"]:
            args[arg[2:]] = next_arg  # Remove -- prefix
            i += 2
        else:
            i += 1

    required_args = ["ws_port", "grpc_port", "priority", "seqnum"]
    missing_args = [arg for arg in required_args if arg not in args]
    if missing_args:
        print(f"Missing required arguments: {missing_args}")
        print(
            "Usage: python chat_server.py --ws_port PORT --grpc_port PORT --priority NUM --seqnum NUM"
        )
        sys.exit(1)

    try:
        # Setup server
        cs = await setup_server(args)

        # Start gRPC server
        grpc_server = await start_grpc_server(cs)

        # Keep gRPC server running in background
        asyncio.create_task(grpc_server.wait_for_termination())

        # Start election lifecycle
        lifecycle_task = await start_election_lifecycle(cs)

        # Start WebSocket server
        ws_port = int(args["ws_port"])

        print(f"\n" + "=" * 60)
        print(f"Server {cs.server_id} started successfully!")
        print(f"WebSocket: ws://0.0.0.0:{ws_port}")
        print(f"gRPC: {cs.grpc_addr}")
        print(f"Priority: {cs.priority}")
        print(f"Sequence Number: {cs.seq_num}")
        print(f"Ring topology: {list(KNOWN_GRPC.keys())}")
        print("=" * 60 + "\n")

        async with serve(cs.handler, "0.0.0.0", ws_port) as ws:
            logging.info(
                "%s: WebSocket server listening on 0.0.0.0:%d", cs.server_id, ws_port
            )

            # Periodically log server status
            async def status_logger():
                while True:
                    await asyncio.sleep(30)  # Log status every 30 seconds
                    status = cs.get_status()
                    logging.info(
                        "%s: Status - Leader: %s, Is Leader: %s, Clients: %d",
                        cs.server_id,
                        status["current_leader"],
                        status["is_leader"],
                        len(status["connected_clients"]),
                    )

            status_task = asyncio.create_task(status_logger())

            try:
                await ws.serve_forever()
            finally:
                status_task.cancel()
                lifecycle_task.cancel()
                await grpc_server.stop(grace=2)

    except KeyboardInterrupt:
        logging.info("Server shutting down due to keyboard interrupt")
    except Exception as e:
        logging.exception("Server startup failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
    