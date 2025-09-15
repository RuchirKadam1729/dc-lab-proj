# from chat_server import known_servers
from protogens.BullyElection_pb2 import *
from protogens.BullyElection_pb2_grpc import *
from pydantic import BaseModel
import grpc, asyncio
from functools import reduce


class BullyElectionImpl(BullyElectionServicer, BaseModel):
    id: str  # server-id
    priority: int
    leader: str = ""
    known_servers: dict[str, str]

    async def Election(self, request, context) -> CandidateMessage:
        return CandidateMessage(self.priority)

    async def Leader(self, request : LeaderMessage, context) -> None:
        self.leader = request.leader_id
        return None

    async def Request(self, request, context) -> ResponseMessage:
        return ResponseMessage(self.priority)

    async def ElectionPhase(self) -> bool:
        async def higher_than(addr):
            async with grpc.aio.insecure_channel(addr) as ch:
                stub = BullyElectionStub(ch)
                resp: CandidateMessage = await asyncio.wait_for(
                    stub.Request(ElectionMessage()), timeout=5
                )
                return resp.priority < self.priority

        tasks = []
        async with asyncio.TaskGroup() as tg:
            for addr in self.known_servers.values():
                tasks.append(tg.create_task(higher_than(addr)))

        return reduce(
            lambda acc, bool: acc and bool, tasks, True
        )  # greater than all, he won election

    async def LeaderPhase(self):
        async def higher_than(addr):
            async with grpc.aio.insecure_channel(addr) as ch:
                stub = BullyElectionStub(ch)
                resp: ResponseMessage = await asyncio.wait_for(
                    stub.Request(LeaderMessage()), timeout=5
                )
                return resp.priority < self.priority

        tasks = []
        async with asyncio.TaskGroup() as tg:
            for addr in self.known_servers.values():
                tasks.append(tg.create_task(higher_than(addr)))

        return reduce(
            lambda acc, bool: acc and bool, tasks, True
        )  # validity-check (mostly goes unused)

    async def RequestPhase(self):
        leader_addr: str = self.known_servers[self.leader]
        async with grpc.aio.insecure_channel(leader_addr) as ch:
            stub = BullyElectionStub(ch)
            while 1:
                resp: ResponseMessage | asyncio.TimeoutError = await asyncio.wait_for(
                    stub.Request(RequestMessage()), timeout=5
                )
                match resp:
                    case RequestMessage():
                        asyncio.sleep(5)
                        continue
                    case asyncio.TimeoutError:
                        break

    async def LifeCycle(self):
        while 1:
            await self.RequestPhase()
            leader: bool = await self.ElectionPhase()
            if leader:
                valid = await self.LeaderPhase()
                if valid == False:
                    break
            else:
                await asyncio.sleep(5)
                continue
