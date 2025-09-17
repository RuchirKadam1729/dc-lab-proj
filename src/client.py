import logging
from pydantic import BaseModel
from ChatServer_pb2 import ChatMessage
import asyncio
from datetime import datetime
import json, uuid
from google.protobuf.json_format import MessageToJson, Parse


class Client:
    def __init__(self, addr, id) -> None:
        self.addr: str = addr
        self.id: str = id
        self.name: str = "Anonymous"
        self.chatMessages: list[ChatMessage] = []  # maybe replaced by db later (shrugs)
        self.v_clock: dict[str, int] = {self.id: 0}
        self.date_time: datetime = datetime.now()

    async def receive(self, chatMessages: ChatMessage):
        self.chatMessages.append(chatMessages)


    async def produce(self):
        to: str = await asyncio.to_thread(input, "to: ")
        str_msg: str = await asyncio.to_thread(input, "msg: ")

        # **increment our local clock for this send event**
        self.v_clock[self.id] = self.v_clock.get(self.id, 0) + 1

        return ChatMessage(
            sender_id=self.id,
            recipient_id=to,
            payload=str_msg,
            date_time=datetime.now(),
            v_clock=dict(self.v_clock),  # send a copy
            msg_id=str(uuid.uuid4()),  # unique id for dedupe
        )

    async def producer_handler(self, websocket):
        from websockets.exceptions import ConnectionClosed

        while True:
            try:
                message = await self.produce()
                message_json = MessageToJson(message)
                await websocket.send(message_json)
            except ConnectionClosed:
                break

    async def consume(self, message: ChatMessage):
        self.chatMessages.append(message)
        print("Received message:", message)

    async def consumer_handler(self, websocket):
        async for message_json in websocket:
            message = Parse(message_json, ChatMessage())
            await self.consume(message)

    async def handler(self, websocket):
        await asyncio.gather(
            self.consumer_handler(websocket), self.producer_handler(websocket)
        )


import sys


async def main():
    args: dict[str, str] = {}

    for i in range(1, len(sys.argv) - 1):
        arg = sys.argv[i]
        next_arg = sys.argv[i + 1]
        match arg:
            case "-p":
                args["port"] = next_arg
            case "--server-addr":
                args["server-addr"] = next_arg
            case "--id":
                args["id"] = next_arg

    client = Client(id=args["id"], addr=f'ws://0.0.0.0:{args["port"]}')
    server_addr = args["server-addr"]
    from websockets.asyncio.client import connect

    async with connect(server_addr) as ws:
        # register
        await ws.send(json.dumps({"client_id": client.id}))
        await client.handler(ws)


if __name__ == "__main__":
    asyncio.run(main())
