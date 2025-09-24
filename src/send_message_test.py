import grpc
from ChatServer_pb2 import ChatMessage
from ChatServer_pb2_grpc import ChatServerStub
from google.protobuf.timestamp_pb2 import Timestamp
import time

def send_message():
    channel = grpc.insecure_channel('localhost:50051')  
    stub = ChatServerStub(channel)

    # Create a Timestamp for current time
    timestamp = Timestamp()
    timestamp.GetCurrentTime()

    message = ChatMessage(
        msg_id="test-msg-007",
        sender_id="client-2",
        recipient_id="client-4",
        payload="Ei-chan !",  
        v_clock={"srv-C": 4},
        date_time=timestamp
    )

    response = stub.Forward(message)

    print(f"Message Forward Response: status_code={response.status_code}")

if __name__ == "__main__":
    send_message()
