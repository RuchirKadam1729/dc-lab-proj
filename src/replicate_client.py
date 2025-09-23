# src/replicate_client.py
import grpc, time
from protos import replication_pb2_grpc, replication_pb2
from google.protobuf.timestamp_pb2 import Timestamp

def replicate_once(peer_addr, chat_msg, origin_node, timeout=2.0):
    """Synchronous replicate to a single peer (used for strong replication)."""
    try:
        chan = grpc.insecure_channel(peer_addr)
        stub = replication_pb2_grpc.ReplicationStub(chan)
        req = replication_pb2.ReplicateRequest(
            msg=replication_pb2.ChatMessage(
                id=chat_msg['id'],
                sender=chat_msg['sender'],
                body=chat_msg['body'],
                ts=chat_msg['ts'],
                vc=replication_pb2.VectorClock(clock=chat_msg['vc'])
            ),
            origin_node=origin_node
        )
        resp = stub.Replicate(req, timeout=timeout)
        return resp.ok, resp.reason
    except Exception as e:
        return False, str(e)
