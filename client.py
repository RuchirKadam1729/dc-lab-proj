import logging
from pydantic import BaseModel
from protogens.ChatServer_pb2 import *
from protogens.ChatServer_pb2_grpc import *
from pydantic import BaseModel
import websockets
import heapq
import asyncio
from datetime import datetime


class Client(BaseModel):
    id: str
    name: str = "Anonymous"

    known_addrs: set[str]
    chatMessages: list[ChatMessage]  # maybe replaced by db later (shrugs)
    v_clock: dict[str, int]
    date_time: datetime


class ClientFunctions:
    def __init__(self, client: Client):
        self.client: Client = client

    def receive(self, chatMessages: ChatMessage):
        self.client.chatMessages.append(chatMessages)


import json


# Work in Progress - 80% done
class WebSocketConn:
    def __init__(self, clientFuncs: ClientFunctions):
        self.clientFuncs = clientFuncs

    async def consume(self, message: ChatMessage):
        event = json.loads(message)
        match event["invoke"]:
            case "receive":
                self.clientFuncs.receive(message)

    async def produce(self, recipient_id, str_message) -> ChatMessage:
        client = self.clientFuncs.client
        return ChatMessage(
            sender_id=client.id,
            recipient_id=recipient_id,
            payload=str_message,
            v_clock=client.v_clock,
            date_time=client.date_time,
        )

    async def consumer_handler(self, websocket):
        async for message in websocket:
            await self.consume(message)

    async def producer_handler(self, websocket):
        while True:
            try:
                message = await self.produce()
                await websocket.send(message)
            except websockets.exceptions.ConnectionClosed:
                break

    async def handler(self, websocket):
        await asyncio.gather(
            self.consumer_handler(websocket), self.producer_handler(websocket)
        )

        async with websockets.asyncio.server.serve(self.handler, "", 8001) as server:
            await server.serve_forever()


def run():
    client = Client(**{"id": "123", "name": "client1"})


if __name__ == "__main__":
    logging.basicConfig()
    run()
