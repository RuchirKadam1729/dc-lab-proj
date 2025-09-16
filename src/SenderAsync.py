import asyncio, websockets
class SenderAsync:
    def __init__(self, websocket):
        self.ws = websocket
        self.q = asyncio.Queue()
        self._task = None

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._writer())

    async def _writer(self):
        try:
            while True:
                msg = await self.q.get()
                await self.ws.send(msg)
        except Exception:
            # placeholder
            pass

    def send(self, msg):
        self.q.put_nowait(msg)
