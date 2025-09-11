import argparse
from types import SimpleNamespace
import types
import grpc
import idl_pb2_grpc
import idl_pb2
import logging
from pydantic import BaseModel


class Client(BaseModel):
    id: str
    name: str = "Anonymous"


class ClientFunctions:
    def __init__(self, client: Client):
        self.client: Client = client

    def say(self) -> str:
        return self.client.name


def run():
    client = Client(**{"id": "123", "name": "client1"})
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = idl_pb2_grpc.GreeterStub(channel)
        resp: idl_pb2.HelloResponse = stub.SayHello(
            idl_pb2.HelloRequest(name=client.name)
        )
        print(f"server says: {resp.message}")


if __name__ == "__main__":
    logging.basicConfig()
    run()
