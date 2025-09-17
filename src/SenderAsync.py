import asyncio
import uuid
from typing import Optional


class SenderAsync:
    """
    Single-writer wrapper around a websocket-like connection.

    - send(msg): non-blocking, can be called from anywhere on the same loop (or use loop.call_soon_threadsafe from other threads).
    - send_and_wait(msg): awaitable — resolves when the message has been enqueued; optionally you can extend to wait for it to be actually sent.
    - start(): create the writer task (call once, from the connection handler).
    - close(): close writer gracefully.
    """

    def __init__(self, websocket, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.ws = websocket
        self.q: asyncio.Queue = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
        self._loop = loop or asyncio.get_running_loop()
        self._closed = False

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._writer())

    async def _writer(self):
        try:
            while True:
                item = await self.q.get()
                if item is None:  # sentinel => shutdown
                    break
                msg, fut = item  # fut may be None or an asyncio.Future
                try:
                    # await websocket.send only here, in single writer
                    await self.ws.send(msg)
                    if fut is not None and not fut.cancelled():
                        fut.set_result(True)
                except Exception as e:
                    # surface error to waiter if any
                    if fut is not None and not fut.cancelled():
                        fut.set_exception(e)
                    # let handler detect disconnection (writer will exit on error)
                    break
        finally:
            self._closed = True
            # fail any remaining waiters
            while not self.q.empty():
                try:
                    _, fut = self.q.get_nowait()
                    if fut is not None and not fut.cancelled():
                        fut.set_exception(RuntimeError("sender closed"))
                except Exception:
                    break

    def send(self, msg: bytes | str):
        """
        Non-blocking enqueue. Use this for the C-like write() behavior.
        """
        if self._closed:
            raise RuntimeError("sender closed")
        # no need to await: put_nowait is fine because queue is unbounded by default
        self.q.put_nowait((msg, None))

    async def send_and_wait(
        self, msg: bytes | str, timeout: Optional[float] = None
    ) -> bool:
        """
        Awaitable: returns True if enqueued and later successfully sent (if writer succeeded).
        This creates a Future that the writer will resolve after actual ws.send() finishes.
        """
        if self._closed:
            raise RuntimeError("sender closed")
        fut = self._loop.create_future()
        self.q.put_nowait((msg, fut))
        # wait for writer to actually attempt sending
        return await asyncio.wait_for(fut, timeout=timeout)

    async def close(self):
        # enqueue sentinel and wait for writer to finish
        if self._task is None:
            return
        await self.q.put(None)
        await self._task
