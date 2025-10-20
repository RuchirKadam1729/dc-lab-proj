#!/usr/bin/env python3
# chat_server.py (Bully Election Version) - patched to add replication modes and /status endpoint
# NOTE: I preserved all original logic; additions are clearly marked with comments.
import asyncio
import json
import logging
import heapq
from dataclasses import dataclass
from datetime import datetime
from random import randbytes
from typing import List, Dict, Optional
import os
import sqlite3
import threading
import random
import time

import grpc
from google.protobuf.json_format import MessageToJson, Parse
from google.protobuf.empty_pb2 import Empty
from websockets.asyncio.server import serve

# gRPC protos
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

# New imports for status API (original)
from fastapi import FastAPI
import uvicorn

# ----- PRIMARY-BACKUP ADDITIONS (paste after existing imports) -----
from fastapi import FastAPI, Body
from fastapi.responses import JSONResponse
from uuid import uuid4
import httpx

# Job states
class JobState:
    def __init__(self):
        self.job_id: Optional[str] = None
        self.phase: str = "idle"   # idle | replicated | processing | finished | failed
        self.started_at: Optional[float] = None
        self.processing_ms: int = 0
        self.result: Optional[str] = None

# Simple thread-safe wrapper for job processing
class JobProcessor:
    def __init__(self):
        # The async lock is used if any async code wants to await; thread_lock for threads.
        self._lock = asyncio.Lock()
        self._thread_lock = threading.Lock()
        self.state = JobState()

    def start_job_thread(self, processing_ms: int, role_update_cb=None, job_id: Optional[str] = None):
        """Start job in a background thread to simulate long processing"""
        with self._thread_lock:
            if self.state.phase in ("processing", "replicated"):
                # Already processing
                return False, "already_processing"
            self.state.job_id = job_id or uuid4().hex
            self.state.phase = "processing"
            self.state.started_at = time.time()
            self.state.processing_ms = processing_ms
            self.state.result = None

            def _run():
                try:
                    # sleep to simulate long processing (we use smaller sleeps so monitor can intervene)
                    remaining = processing_ms / 1000.0
                    slice_sec = 0.5
                    while remaining > 0:
                        time.sleep(min(slice_sec, remaining))
                        remaining -= slice_sec
                    self.state.phase = "finished"
                    self.state.result = f"completed (proc_ms={processing_ms})"
                    # call callback if provided (e.g., to signal promotion policy)
                    if role_update_cb:
                        try:
                            role_update_cb()
                        except Exception:
                            pass
                except Exception as ex:
                    self.state.phase = "failed"
                    self.state.result = f"failed: {ex}"

            t = threading.Thread(target=_run, daemon=True)
            t.start()
            return True, self.state.job_id

    def accept_replicated_job(self, processing_ms: int, job_id: Optional[str] = None):
        """Backup accepts replication and starts processing concurrently."""
        with self._thread_lock:
            # If already processing same job, ignore
            if self.state.job_id == job_id and self.state.phase == "processing":
                return True, self.state.job_id
            self.state.job_id = job_id or uuid4().hex
            self.state.phase = "processing"
            self.state.started_at = time.time()
            self.state.processing_ms = processing_ms
            self.state.result = None
            # start background same as above
            def _run():
                try:
                    remaining = processing_ms / 1000.0
                    slice_sec = 0.5
                    while remaining > 0:
                        time.sleep(min(slice_sec, remaining))
                        remaining -= slice_sec
                    self.state.phase = "finished"
                    self.state.result = f"completed (proc_ms={processing_ms})"
                except Exception as ex:
                    self.state.phase = "failed"
                    self.state.result = f"failed: {ex}"
            t = threading.Thread(target=_run, daemon=True)
            t.start()
            return True, self.state.job_id

    def get_status(self):
        return {
            "job_id": self.state.job_id,
            "phase": self.state.phase,
            "started_at": self.state.started_at,
            "processing_ms": self.state.processing_ms,
            "result": self.state.result,
        }
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# ---------------------------
# Simple SQLite-backed Store
# ---------------------------
class Store:
    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init()

    def _init(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS msgs (
                id TEXT PRIMARY KEY,
                sender TEXT,
                recipient TEXT,
                body TEXT,
                ts INTEGER,
                vc TEXT
            )
            """
        )
        self.conn.commit()

    def save_message(self, rec: dict):
        self.conn.execute(
            "INSERT OR IGNORE INTO msgs (id,sender,recipient,body,ts,vc) VALUES (?,?,?,?,?,?)",
            (
                rec.get("id"),
                rec.get("sender"),
                rec.get("recipient_id"),
                rec.get("body"),
                rec.get("ts"),
                json.dumps(rec.get("v_clock", {})),
            ),
        )
        self.conn.commit()

    def list_messages(self, limit: int = 100) -> List[dict]:
        cur = self.conn.execute(
            "SELECT id,sender,recipient,body,ts,vc FROM msgs ORDER BY ts LIMIT ?",
            (limit,),
        )
        out = []
        for r in cur:
            out.append(
                {
                    "id": r[0],
                    "sender": r[1],
                    "recipient_id": r[2],
                    "body": r[3],
                    "ts": r[4],
                    "v_clock": json.loads(r[5]) if r[5] else {},
                }
            )
        return out


@dataclass
class MsgBuffer:
    buf: List[MsgBufferNode]

    def buffer_in(self, msg: ChatMessage):
        heapq.heappush(self.buf, MsgBufferNode(msg))

    def buffer_out(self) -> ChatMessage:
        return heapq.heappop(self.buf).val


# Define the server topology with priorities
# Higher priority numbers win in bully algorithm
SERVER_TOPOLOGY = [
    # (server_id, grpc_addr_for_peers, ws_bind_addr, priority)
    ("srv-A", "chat_node1:50051", "0.0.0.0:9001", 9),
    ("srv-B", "chat_node2:50052", "0.0.0.0:9002", 5),
    ("srv-C", "chat_node3:50053", "0.0.0.0:9003", 1),
]

# Create dictionaries from topology
from collections import OrderedDict

KNOWN_GRPC = OrderedDict(
    [(srv_id, grpc_addr) for srv_id, grpc_addr, _, _ in SERVER_TOPOLOGY]
)
KNOWN_WS = OrderedDict([(srv_id, ws_addr) for srv_id, _, ws_addr, _ in SERVER_TOPOLOGY])
SERVER_PRIORITIES = {srv_id: priority for srv_id, _, _, priority in SERVER_TOPOLOGY}

# Create port mappings for server identification
PORT_TO_SERVER_ID = {
    "50051": "srv-A",
    "50052": "srv-B",
    "50053": "srv-C",
}


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

        # keep server mappings
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

        # ----- primary-backup fields -----
        # role: "primary", "backup", "monitor" (default assigned by topology / CLI)
        self.role: str = "primary" if server_id == "srv-A" else ("backup" if server_id == "srv-B" else "monitor")
        # job processor handles simulated long-running jobs and replicated acceptance
        self._job_processor = JobProcessor()
        # a simple flag used by monitor to know whether this node is alive/unresponsive
        self.last_heartbeat = time.time()
        # control port for PB HTTP API (computed from grpc_port to avoid collisions)
        # if you pass --control_port in args, it will override this default
        try:
            self.control_port = int(self.grpc_addr.split(":")[1]) + 3000
        except Exception:
            self.control_port = None
        # Note: prefer explicit control_port via CLI/docker-compose; above is fallback.
        # -------------------------------------------------------

        # Replication configuration (can override via env vars)
        # modes: none | strong | eventual
        self.replication_mode = os.environ.get("REPLICATION_MODE", "none").lower()
        self.replication_acks = os.environ.get("REPLICATION_ACKS", "majority").lower()
        # derived peer list (addresses only)
        self.peers = [addr for sid, addr in (self.known_grpc_servers or KNOWN_GRPC).items() if sid != self.server_id]

        # Persistent store (per-node)
        db_path = os.environ.get("STORE_DB_PATH", f"./data/{self.server_id}/store.db")
        self.store = Store(db_path)

        logging.info(
            "ChatServer created: server_id=%s, unique_id=%s, priority=%s, grpc_addr=%s",
            self.server_id,
            self.unique_id,
            self.priority,
            self.grpc_addr,
        )

        # Create bully election implementation
        self.bully_election_impl: BullyElectionImpl = BullyElectionImpl(
            server_id=self.server_id,
            priority=self.priority,
            known_servers=self.known_grpc_servers,
            self_grpc_addr=self.grpc_addr,
        )

        # background tasks placeholders
        self._gossip_task: Optional[asyncio.Task] = None

    # ChatServer gRPC method
    async def Forward(self, request: ChatMessage, context=None) -> ChatServerResponse:
        """
        NOTE: This method now includes optional replication behaviour.
        - If called *locally* (context is None) it means message arrived from websocket handler.
        - If self.replication_mode == 'strong' AND this node is leader AND context is None -> replicate synchronously to peers before delivering/queuing.
        - If self.replication_mode == 'eventual' AND context is None -> persist locally and let gossip handle replication.
        """
        # detect replication RPC via metadata (so we can allow client gRPC to trigger replication)
        is_replication_rpc = False
        if context is not None:
            try:
                md = dict(context.invocation_metadata() or [])
                # metadata keys are lowercased by gRPC; we look for 'replica' == '1'
                if md.get("replica") == "1":
                    is_replication_rpc = True
            except Exception:
                # tolerate any introspection errors by treating as non-replication RPC
                is_replication_rpc = False

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

        # If eventual replication: persist immediately and let background gossip push
        if self.replication_mode == "eventual" and context is None:
            # save to local store for anti-entropy
            body_val = getattr(request, "payload", None)
            rec = {
                "id": request.msg_id,
                "sender": request.sender_id,
                "recipient_id": request.recipient_id,
                "body": body_val,
                "ts": int(datetime.utcnow().timestamp() * 1000),
                "v_clock": dict(request.v_clock),
            }
            try:
                self.store.save_message(rec)
            except Exception as e:
                logging.exception("%s: failed to persist message for eventual replication: %s", self.server_id, e)

        # Strong replication (primary-backup)
        if (
            self.replication_mode == "strong"
            and not is_replication_rpc
            and self.bully_election_impl.is_leader()
        ):
            # replicate synchronously to peers and wait for ACKs depending on policy
            peer_addrs = [addr for sid, addr in (self.known_grpc_servers or KNOWN_GRPC).items() if sid != self.server_id]
            if peer_addrs:
                ok_acks = 0
                total_peers = len(peer_addrs)

                # required ack calculation (majority by default)
                if self.replication_acks == "all":
                    required = total_peers
                else:  # majority
                    total_nodes = len(KNOWN_GRPC)
                    required = (total_nodes // 2) + 1
                    required_acks_from_peers = max(0, required - 1)

                # synchronous replicate in executor to avoid blocking event loop
                async def replicate_to_peer(peer_addr):
                  try:
                      def _call():
                          chan = grpc.insecure_channel(peer_addr)
                          stub = ChatServerStub(chan)
                          # increase timeout and get response
                          # mark the outgoing RPC as replication via metadata to avoid re-replication loops
                          resp = stub.Forward(request, timeout=5.0, metadata=(('replica','1'),))
                          return resp
              
                      loop = asyncio.get_running_loop()
                      resp = await loop.run_in_executor(None, _call)
              
                      # log the response from the peer
                      logging.info(
                          "%s: replicate to %s returned status=%s payload=%s",
                          self.server_id,
                          peer_addr,
                          getattr(resp, "status_code", None),
                          getattr(resp, "payload", None),
                      )
              
                      # consider replication successful only if peer persisted or already has it (DUP)
                      accepted = {
                          ChatServerResponse.DELIVERED_LOCAL,
                          ChatServerResponse.QUEUED_LOCAL,
                          ChatServerResponse.DELIVERED_REMOTE,
                          ChatServerResponse.QUEUED_REMOTE,
                          ChatServerResponse.QUEUED_FALLBACK,
                          ChatServerResponse.DUP,
                      }
                      if getattr(resp, "status_code", None) in accepted:
                          return True
                      logging.warning("%s: replicate to %s returned non-persistent status %s", self.server_id, peer_addr, getattr(resp, "status_code", None))
                      return False
                  except Exception as e:
                      logging.exception("%s: replicate to %s failed: %s", self.server_id, peer_addr, e)
                      return False

                tasks = [replicate_to_peer(p) for p in peer_addrs]
                results = await asyncio.gather(*tasks)
                ok_acks = sum(1 for r in results if r)
                required = (len(KNOWN_GRPC) // 2) + 1
                required_from_peers = max(0, required - 1)

                if ok_acks < required_from_peers:
                    # insufficient replication - strong policy says we should not commit/deliver
                    logging.warning(
                        "%s: insufficient replication acks (%d/%d) for msg %s - required %d",
                        self.server_id,
                        ok_acks,
                        total_peers,
                        request.msg_id,
                        required,
                    )

                    # Persist the message so queued messages survive restarts/crashes
                    body_val = getattr(request, "payload", None)
                    rec = {
                        "id": request.msg_id,
                        "sender": request.sender_id,
                        "recipient_id": request.recipient_id,
                        "body": body_val,
                        "ts": int(datetime.utcnow().timestamp() * 1000),
                        "v_clock": dict(request.v_clock),
                    }
                    try:
                        self.store.save_message(rec)
                    except Exception as e:
                        logging.exception("%s: failed to persist queued (insufficient-acks) message %s: %s", self.server_id, request.msg_id, e)

                    # Queue in memory for delivery when available
                    self.msgBuffers[request.recipient_id].buffer_in(request)
                    return ChatServerResponse(status_code=ChatServerResponse.QUEUED_LOCAL)
                else:
                    # sufficient replication - fallthrough to deliver/queue below
                    logging.info(
                           "%s: Forward called; msg_id=%s sender=%s recipient=%s payload=%s v_clock=%s",
                           self.server_id,
                           getattr(request, "msg_id", None),
                           getattr(request, "sender_id", None),
                           getattr(request, "recipient_id", None),
                           getattr(request, "payload", None),
                           dict(request.v_clock),
                    )
                    logging.debug("%s: Forward request JSON: %s", self.server_id, MessageToJson(request))

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
            # persist to store as delivered
            try:
                body_val = getattr(request, "payload", None)
                rec = {
                    "id": request.msg_id,
                    "sender": request.sender_id,
                    "recipient_id": request.recipient_id,
                    "body": body_val,
                    "ts": int(datetime.utcnow().timestamp() * 1000),
                    "v_clock": dict(request.v_clock),
                }
                self.store.save_message(rec)
            except Exception:
                logging.exception("%s: failed to persist delivered message %s", self.server_id, request.msg_id)

            return ChatServerResponse(status_code=ChatServerResponse.DELIVERED_LOCAL)

        # recipient not connected -> persist + queue locally
        body_val = getattr(request, "payload", None)
        rec = {
            "id": request.msg_id,
            "sender": request.sender_id,
            "recipient_id": request.recipient_id,
            "body": body_val,
            "ts": int(datetime.utcnow().timestamp() * 1000),
            "v_clock": dict(request.v_clock),
        }
        try:
            self.store.save_message(rec)
        except Exception:
            logging.exception("%s: failed to persist queued message %s", self.server_id, request.msg_id)

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
        """Get server status including election info and replication status"""
        bully_status = self.bully_election_impl.get_status()
        replication_info = {
            "mode": self.replication_mode,
            "peers": list(self.peers),
            "store_messages": len(self.store.list_messages(1000)),
        }
        # include primary-backup job status
        pb_job = None
        try:
            pb_job = self._job_processor.get_status()
        except Exception:
            pb_job = None
        return {
            "server_id": self.server_id,
            "unique_id": self.unique_id,
            "priority": self.priority,
            "grpc_addr": self.grpc_addr,
            "ws_addr": self.ws_addr,
            "current_leader": self.bully_election_impl.get_current_leader(),
            "is_leader": self.bully_election_impl.is_leader(),
            "connected_clients": list(self.clients.keys()),
            "queued_messages": {
                client_id: len(buf.buf) for client_id, buf in self.msgBuffers.items()
            },
            "bully_election_status": bully_status,
            "replication": replication_info,
            "pb": {
                "role": self.role,
                "job": pb_job,
                "control_port": self.control_port,
            },
        }

    # Start background gossip task (eventual replication)
    def start_gossip_loop(self, interval: int = 5):
        if self.replication_mode != "eventual":
            return
        if self._gossip_task is None:
            loop = asyncio.get_event_loop()
            self._gossip_task = loop.create_task(self._gossip_worker(interval))
            logging.info("%s: started gossip worker (interval=%ds)", self.server_id, interval)

    async def _gossip_worker(self, interval: int):
        # very simple anti-entropy: pick a random peer and push latest stored messages
        while True:
            try:
                await asyncio.sleep(interval)
                if not self.peers:
                    continue
                peer = random.choice(self.peers)
                # fetch recent messages
                recent = self.store.list_messages(50)
                if not recent:
                    continue

                # send messages one-by-one to peer using blocking stub in executor
                async def push_one(msg):
                    try:
                        def _call():
                            chan = grpc.insecure_channel(peer)
                            stub = ChatServerStub(chan)
                            # reconstruct ChatMessage from dict
                            pb = ChatMessage()
                            pb.msg_id = msg["id"]
                            pb.sender_id = msg["sender"]
                            pb.recipient_id = msg["recipient_id"]
                            # payload field in proto
                            pb.payload = msg.get("body", "")
                            # v_clock is a map field - set accordingly
                            pb.v_clock.clear()
                            pb.v_clock.update(msg.get("v_clock", {}))
                            resp = stub.Forward(pb, timeout=2.0)
                            return resp

                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, _call)
                        logging.debug("%s: gossip pushed msg %s -> %s", self.server_id, msg["id"], peer)
                    except Exception:
                        logging.debug("%s: gossip failed pushing to %s", self.server_id, peer)

                tasks = [push_one(m) for m in recent]
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                logging.info("%s: gossip worker cancelled", self.server_id)
                break
            except Exception as e:
                logging.debug("%s: gossip worker error: %s", self.server_id, e)


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

    # Use the predefined priority for this server, but allow override from command line
    default_priority = SERVER_PRIORITIES[server_id]
    actual_priority = int(args.get("priority", default_priority))

    logging.info(
        "Setting up server: id=%s, grpc=%s, ws=%s, priority=%s",
        server_id,
        grpc_addr,
        ws_addr,
        actual_priority,
    )

    cs = ChatServer(
        server_id=server_id,
        ws_addr=ws_addr,
        grpc_addr=grpc_addr,
        priority=actual_priority,
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
    add_BullyElectionServicer_to_server(cs.bully_election_impl, grpc_server)

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
    """Start the bully election lifecycle"""

    def _on_lifecycle_done(task: asyncio.Task):
        try:
            exc = task.exception()
            if exc:
                logging.exception(
                    "%s: Bully election lifecycle task failed",
                    cs.server_id,
                    exc_info=exc,
                )
            else:
                logging.info(
                    "%s: Bully election lifecycle finished normally", cs.server_id
                )
        except asyncio.CancelledError:
            logging.info("%s: Bully election lifecycle cancelled", cs.server_id)

    # Start background lifecycle task
    lifecycle_task = asyncio.create_task(cs.bully_election_impl.LifeCycle())
    lifecycle_task.add_done_callback(_on_lifecycle_done)

    logging.info("%s: started Bully election lifecycle task", cs.server_id)

    # Run one immediate election round for faster startup
    asyncio.create_task(cs.bully_election_impl.StartElection())

    return lifecycle_task


# -------------------------
# Status HTTP (FastAPI)
# -------------------------
app = FastAPI()

# We'll wire the ChatServer instance at runtime into this module-level var
_RUNTIME_CS: Optional[ChatServer] = None

@app.get("/status")
async def status():
    if _RUNTIME_CS is None:
        return {"error": "server not ready"}
    return _RUNTIME_CS.get_status()


# ----------------- PRIMARY-BACKUP CONTROL HTTP API -----------------
pb_app = FastAPI(title="PrimaryBackup-Control")

@pb_app.get("/pb/status")
async def pb_status():
    # return role and job status
    cs_obj = globals().get("cs")
    if not cs_obj:
        return JSONResponse({"error": "server not ready"}, status_code=503)
    return JSONResponse({
        "server_id": cs_obj.server_id,
        "role": cs_obj.role,
        "job": cs_obj._job_processor.get_status(),
        "time": time.time(),
    })

@pb_app.post("/pb/set_role")
async def pb_set_role(payload: dict = Body(...)):
    # payload: {"role": "primary" | "backup" | "monitor"}
    cs_obj = globals().get("cs")
    if not cs_obj:
        return JSONResponse({"error": "server not ready"}, status_code=503)
    new_role = payload.get("role")
    if new_role not in ("primary", "backup", "monitor"):
        return JSONResponse({"error": "invalid role"}, status_code=400)
    cs_obj.role = new_role
    return {"ok": True, "role": cs_obj.role}

@pb_app.post("/pb/forward_job")
async def pb_forward_job(payload: dict = Body(...)):
    """
    Called by primary to forward/replicate a job to this backup node.
    payload: {"job_id": "...", "processing_ms": int}
    Backup accepts and immediately starts processing; reply ack.
    """
    cs_obj = globals().get("cs")
    if not cs_obj:
        return JSONResponse({"error": "server not ready"}, status_code=503)
    job_id = payload.get("job_id")
    processing_ms = int(payload.get("processing_ms", 0))
    # Only backups accept forwarded jobs (or primaries acting as backup in some topologies)
    accepted, jid = cs_obj._job_processor.accept_replicated_job(processing_ms=processing_ms, job_id=job_id)
    if not accepted:
        return JSONResponse({"ok": False, "reason": "cannot_accept"}, status_code=503)
    return {"ok": True, "job_id": jid}

@pb_app.post("/pb/start_job")
async def pb_start_job(payload: dict = Body(...)):
    """
    Client asks this node to start a long job.
    - If primary: forward to backup, wait for ack of replication, then start processing (simulate).
    - If backup or monitor: return redirect info to client (who should talk to primary)
    payload: {"processing_ms": <ms>}
    """
    cs_obj = globals().get("cs")
    if not cs_obj:
        return JSONResponse({"error": "server not ready"}, status_code=503)
    processing_ms = int(payload.get("processing_ms", 120000))  # ms; default 120000 (2 min)
    # If this node is primary -> forward to backup first
    if cs_obj.role != "primary":
        # return the current primary (ask other nodes via known_grpc) so client can retry
        current_primary = cs_obj.bully_election_impl.get_current_leader()
        return JSONResponse({"ok": False, "reason": "not_primary", "primary": current_primary}, status_code=409)

    # find backup (simple selection: any known server that is not this server)
    backup = None
    backup_grpc = None
    for sid, gaddr in cs_obj.known_grpc_servers.items():
        if sid != cs_obj.server_id:
            backup = sid
            backup_grpc = gaddr
            break
    if not backup:
        return JSONResponse({"ok": False, "reason": "no_backup"}, status_code=503)

    # Forward replication to backup -> call its control endpoint
    try:
        # Derive backup host (we expect known_grpc_servers entries like "chat_node2:50052")
        b_addr = (cs_obj.known_grpc_servers.get(backup) or cs_obj.known_ws_servers.get(backup))
        if not b_addr:
            return JSONResponse({"ok": False, "reason": "no_backup_addr"}, status_code=500)
        b_host = b_addr.split(":")[0]
        backup_control_port = cs_obj.control_port
        if backup_control_port is None:
            return JSONResponse({"ok": False, "reason": "no_control_port_configured"}, status_code=500)
        forward_url = f"http://{b_host}:{backup_control_port}/pb/forward_job"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(forward_url, json={"job_id": uuid4().hex, "processing_ms": processing_ms})
            if resp.status_code != 200:
                return JSONResponse({"ok": False, "reason": "backup_reject", "status": resp.text}, status_code=502)
            data = resp.json()
            # if acked by backup, start processing locally
            ok, jid = cs_obj._job_processor.start_job_thread(processing_ms=processing_ms, job_id=data.get("job_id"))
            return {"ok": True, "replicated_to": backup, "job_id": jid}
    except Exception as ex:
        return JSONResponse({"ok": False, "reason": "forward_error", "error": str(ex)}, status_code=502)

# Helper to run pb_app as background uvicorn in this container if control_port provided as CLI arg
def start_pb_control_server(control_port):
    if not control_port:
        logging.info("PB control server not started (no control_port provided)")
        return
    # run uvicorn server in background thread
    def _run():
        uvicorn.run(pb_app, host="0.0.0.0", port=int(control_port), log_level="info")
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    logging.info("Started PB control server on port %s", control_port)
# -------------------------------------------------------------------

async def main():
    import sys

    # Parse command line arguments
    args: dict[str, str] = {}
    i = 1
    while i < len(sys.argv) - 1:
        arg = sys.argv[i]
        next_arg = sys.argv[i + 1]
        if arg in ["--ws_port", "--grpc_port", "--priority", "--seqnum", "--control_port"]:
            args[arg[2:]] = next_arg  # Remove -- prefix
            i += 2
        else:
            i += 1

    required_args = ["ws_port", "grpc_port", "seqnum"]
    missing_args = [arg for arg in required_args if arg not in args]
    if missing_args:
        print(f"Missing required arguments: {missing_args}")
        print(
            "Usage: python chat_server.py --ws_port PORT --grpc_port PORT [--priority NUM] --seqnum NUM [--control_port PORT]"
        )
        print(
            "Note: If priority is not specified, default priority for the server will be used"
        )
        sys.exit(1)

    try:
        # Setup server
        cs = await setup_server(args)

        # Expose cs to status API
        global _RUNTIME_CS
        _RUNTIME_CS = cs

        # Expose to PB endpoints too (global variable name 'cs')
        globals()['cs'] = cs

        # If control_port passed via CLI, override cs.control_port
        control_port_arg = args.get("control_port")
        if control_port_arg:
            try:
                cs.control_port = int(control_port_arg)
            except Exception:
                logging.warning("Invalid control_port provided, ignoring")

        # start PB control server (only if cs.control_port configured)
        start_pb_control_server(cs.control_port)

        # Start gRPC server
        grpc_server = await start_grpc_server(cs)

        # Keep gRPC server running in background
        asyncio.create_task(grpc_server.wait_for_termination())

        # Start election lifecycle
        lifecycle_task = await start_election_lifecycle(cs)

        # Start gossip loop if eventual
        cs.start_gossip_loop(interval=int(os.environ.get("GOSSIP_INTERVAL", 5)))

        # Start WebSocket server
        ws_port = int(args["ws_port"])

        # Start status HTTP server in background thread
        status_port = int(os.environ.get("STATUS_PORT", 8000 + int(args.get("seqnum", 0))))

        def _run_status():
            uvicorn.run(app, host="0.0.0.0", port=status_port, log_level="info")

        t = threading.Thread(target=_run_status, daemon=True)
        t.start()

        print(f"" + "=" * 70)
        print(f"Chat Server with Bully Election Algorithm (patched with replication + PB)")
        print(f"=" * 70)
        print(f"Server ID: {cs.server_id}")
        print(f"Priority: {cs.priority} (higher wins)")
        print(f"Role: {cs.role}")
        print(f"WebSocket: ws://0.0.0.0:{ws_port}")
        print(f"gRPC: {cs.grpc_addr}")
        print(f"Sequence Number: {cs.seq_num}")
        print(f"Known servers: {list(KNOWN_GRPC.keys())}")
        print(f"Server priorities: {SERVER_PRIORITIES}")
        print(f"Replication mode: {cs.replication_mode}")
        print(f"Status endpoint: http://0.0.0.0:{status_port}/status")
        if cs.control_port:
            print(f"PB control endpoint: http://0.0.0.0:{cs.control_port}/pb/status")
        print(f"=" * 70 + "")

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
                        "%s: Status - Leader: %s, Is Leader: %s, Clients: %d, Priority: %d",
                        cs.server_id,
                        status["current_leader"],
                        status["is_leader"],
                        len(status["connected_clients"]),
                        cs.priority,
                    )

            status_task = asyncio.create_task(status_logger())

            try:
                await ws.serve_forever()
            finally:
                status_task.cancel()
                lifecycle_task.cancel()
                if cs._gossip_task:
                    cs._gossip_task.cancel()
                await grpc_server.stop(grace=2)

    except KeyboardInterrupt:
        logging.info("Server shutting down due to keyboard interrupt")
    except Exception as e:
        logging.exception("Server startup failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
