import time
import asyncio
import threading
from typing import Any
from collections import deque


class Timeout(Exception):

    def __init__(self):
        msg = "No item became available within the specified time."
        super().__init__(msg)


class EvictingQueue:

    def __init__(self, maxsize: int=1):
        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        self._deque = deque()
        self._maxsize = maxsize

    def _put(self, item: Any):
        self._deque.append(item)

    def _get(self) -> Any:
        return self._deque.popleft()

    def put(self, item: Any):
        with self.mutex:
            if len(self._deque) >= self._maxsize:
                self._get()
            self._put(item)
            self.not_empty.notify()

    def get(self, timeout_sec: float=30.0) -> Any:
        with self.not_empty:
            endtime = time.monotonic() + timeout_sec
            while not self._deque:
                remaining = endtime - time.monotonic()
                if remaining <= 0:
                    raise Timeout
                self.not_empty.wait(remaining)
            item = self._get()
            return item


class AsyncEvictingQueue:

    def __init__(self, maxsize: int=1):
        self.mutex = asyncio.Lock()
        self.not_empty = asyncio.Condition(self.mutex)
        self._deque = deque()
        self._maxsize = maxsize

    async def _put(self, item):
        self._deque.append(item)

    async def _get(self) -> Any:
        return self._deque.popleft()

    async def put(self, item):
        async with self.mutex:
            if len(self._deque) >= self._maxsize:
                await self._get()
            await self._put(item)
            self.not_empty.notify()

    async def get(self, timeout_sec: float=30.0) -> Any:
        async with self.not_empty:
            try:
                await asyncio.wait_for(self.not_empty.wait(), timeout_sec)
            except asyncio.TimeoutError:
                raise Timeout
            item = await self._get()
            return item
