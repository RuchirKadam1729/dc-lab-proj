import grpc
from ChatServer_pb2 import ChatMessage
from ChatServer_pb2_grpc import ChatServerStub
from google.protobuf.timestamp_pb2 import Timestamp
import time
from uuid import uuid4


def send_message():
    channel = grpc.insecure_channel("localhost:50051")
    stub = ChatServerStub(channel)

    # Create a Timestamp for current time
    timestamp = Timestamp()
    timestamp.GetCurrentTime()

    message = ChatMessage(
        msg_id=f"test-msg-{uuid4()}",
        sender_id="client-" + input("client id no.: "),
        recipient_id="client-" + input("to client id no.:"),
        payload=input("payload: "),
        v_clock={"srv-" + input("connect to srv-: "): 0},
        date_time=timestamp,
    )

    response = stub.Forward(message)
    print(f"Message Forward Response: status_code={response.status_code}")
    response = stub.Forward(message) # accidental duplicate!!
    print(f"Message Forward Response: status_code={response.status_code}")


if __name__ == "__main__":
    send_message()
