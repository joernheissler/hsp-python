import trio
from heapq import heappush, heappop
from collections import deque
from random import randint


class SendQueue:
    def __init__(self):
        self._not_empty = trio.Event()
        self._queue = []
        self._counter = 0

    def put(self, prio, item):
        heappush(self._queue, (prio, self._counter, item))
        self._counter += 1
        if not self._not_empty.is_set():
            self._not_empty.set()

    def __iter__(self):
        return self

    def __next__(self):
        if not self._queue:
            raise StopIteration()

        __, __, item = heappop(self._queue)

        if not self._queue:
            self._not_empty.clear()

        return item

    async def wait_nonempty(self):
        await self._not_empty.wait()

    def __len__(self):
        return len(self._queue)


class PingQueue:
    def __init__(self):
        self._queue = deque()

    def add(self, msg):
        self._queue.append(msg)

    def remove(self, msg):
        return self._queue.remove(msg)

    def get(self):
        return self._queue[0]

    def __len__(self):
        return len(self._queue)
