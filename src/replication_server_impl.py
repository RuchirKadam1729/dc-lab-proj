# src/replication_server_impl.py
from protos import replication_pb2_grpc, replication_pb2
import sqlite3, json, time

class ReplicationServicer(replication_pb2_grpc.ReplicationServicer):
    def __init__(self, store):
        self.store = store  # e.g., sqlite connection or dict

    def Replicate(self, request, context):
        msg = request.msg
        # convert pb -> dict
        rec = {
            "id": msg.id,
            "sender": msg.sender,
            "body": msg.body,
            "ts": msg.ts,
            "vc": dict(msg.vc.clock)
        }
        try:
            self.store.save_message(rec)  # implement this
        except Exception as e:
            return replication_pb2.ReplicateResponse(ok=False, reason=str(e))
        return replication_pb2.ReplicateResponse(ok=True)
