import logging
from pydantic import BaseModel
from protos.ChatServer_pb2 import ChatMessage

# from protos.ChatServer_pb2_grpc import
from pydantic import BaseModel
import websockets
import heapq
import asyncio
from datetime import datetime


class Client:
    def __init__(self, addr, id="abc") -> None:
        self.addr: str = addr
        self.id: str = id
        self.name: str = "Anonymous"
        self.chatMessages: list[ChatMessage] = []  # maybe replaced by db later (shrugs)
        self.v_clock: dict[str, int] = {}
        self.date_time: datetime = datetime.now()

    async def receive(self, chatMessages: ChatMessage):
        self.chatMessages.append(chatMessages)

    async def produce(self):
        to: str = await asyncio.to_thread(input, "to: ")
        str_msg: str = await asyncio.to_thread(input, "msg: ")
        from datetime import datetime

        return ChatMessage(
            sender_id=self.id,
            recipient_id=to,
            payload=str_msg,
            date_time=datetime.now(),
            v_clock=self.v_clock,
        )

    async def producer_handler(self, websocket):
        from websockets.exceptions import ConnectionClosed

        while True:
            try:
                message = await self.produce()
                await websocket.send(message)
            except ConnectionClosed:
                break

    async def consume(self, message: ChatMessage):
        self.chatMessages.append(message)
        print("Received message:", message)

    async def consumer_handler(self, websocket):
        async for message in websocket:
            await self.consume(message)

    async def handler(self, websocket):
        await asyncio.gather(
            self.consumer_handler(websocket), self.producer_handler(websocket)
        )


import sys


async def main():
    client = Client(addr=f"ws://0.0.0.0:{sys.argv[1]}")
    server_addr = sys.argv[2]
    from websockets.asyncio.client import connect

    async with connect(server_addr) as ws:
        await client.handler(ws)


if __name__ == "__main__":
    asyncio.run(main())
