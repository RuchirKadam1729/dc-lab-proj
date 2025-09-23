import grpc
from src.ChatServer_pb2 import ChatMessage
from src.ChatServer_pb2_grpc import ChatServerStub
from google.protobuf.timestamp_pb2 import Timestamp
import time

def send_message():
    channel = grpc.insecure_channel('localhost:50051')  # Connect to leader node (srv-A)
    stub = ChatServerStub(channel)

    # Create a Timestamp for current time
    timestamp = Timestamp()
    timestamp.GetCurrentTime()

    message = ChatMessage(
        msg_id="test-msg-001",
        sender_id="client-1",
        recipient_id="client-2",
        payload="Hello, this is a replication test!",  # Correct field name (not 'message')
        v_clock={"srv-A": 1},
        date_time=timestamp
    )

    response = stub.Forward(message)

    print(f"Message Forward Response: status_code={response.status_code}")

if __name__ == "__main__":
    send_message()
