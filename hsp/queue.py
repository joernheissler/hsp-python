import trio
from heapq import heappush, heappop
from collections import deque
from random import randint


class SendQueue:
    def __init__(self):
        self._not_empty = trio.Event()
        self._queue = []
        self._counter = 0

    def put_nowait(self, prio, item):
        heappush(self._queue, (prio, self._counter, item))
        self._counter += 1
        if not self._not_empty.is_set():
            self._not_empty.set()

    def get_nowait(self):
        if not self._not_empty.is_set():
            raise trio.WouldBlock()

        __, __, item = heappop(self._queue)

        if not self._queue:
            self._not_empty.clear()

        return item

    async def get(self):
        while True:
            await self._not_empty.wait()
            try:
                return self.get_nowait()
            except trio.WouldBlock:
                pass


class PingQueue:
    def __init__(self):
        self._queue = deque()

    def add(self, msg):
        self._queue.append(msg)

    def remove(self, msg):
        return self._queue.remove(msg)

    def get(self):
        return self._queue[0]


class DataQueue:
    def __init__(self, max_msg_id):
        self._max_msg_id = max_msg_id
        self._queue = {}

    def add(self, msg):
        if msg.msg_id is not None:
            raise Exception('Cannot add a message with a message ID')

        msg.msg_id = self.get_unused_id()
        self._queue[msg_id] = msg

    def pop(self, msg):
        return self._queue.pop(msg.msg_id)

    def get(self, msg_id):
        return self._queue[msg_id]

    def get_unused_id(self):
        """
        Get an unused message id for a DATA_ACK message.

        This generates random numbers until an unused one is found.
        For each try, the probability to find an unused ID is at least 25% and usually > 50%.
        """
        id_limit = min(self.max_msg_id, len(self._queue) * 2 + 1)

        if len(self._queue) / id_limit > 0.75:
            raise Exception('Too few Message IDs available')

        while True:
            msg_id = randint(0, max_id - 1)
            if msg_id not in self._queue:
                return msg_id
