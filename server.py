from pydantic import BaseModel
import threading
import idl_pb2
import idl_pb2_grpc
import grpc
import logging
from concurrent import futures
import websockets
import asyncio
import json


# rpc service implementation
class Greeter(idl_pb2_grpc.GreeterServicer):
    def SayHello(self, request: idl_pb2.HelloRequest, context):
        return idl_pb2.HelloResponse(f"fuck off {request.name} I'm BUSY.")


class Server(BaseModel):
    id: str
    name: str = "server"


def rpc_serve():
    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    idl_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


async def WS_serve():
    async def consume(message):
        return 200

    async def produce():
        pass

    async def consumer_handler(websocket):
        async for message in websocket:
            await consume(message)

    async def producer_handler(websocket):
        while True:
            try:
                message = await produce()
                await websocket.send(message)
            except websockets.exceptions.ConnectionClosed:
                break

    async def handler(websocket):
        await asyncio.gather(consumer_handler(websocket), producer_handler(websocket))

    async with websockets.asyncio.server.serve(handler, "", 8001) as server:
        await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig()
    threading.Thread(
        target=rpc_serve
    )  # rpc runs with threading, not bothering to mess with it

    asyncio.run(WS_serve())
