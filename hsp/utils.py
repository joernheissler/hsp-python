import trio
from .exception import InvalidOperation, QueueFull
from collections import deque


class Future:
    def __init__(self):
        self._done = trio.Event()
        self._result = None
        self._error = None

    async def wait(self):
        await self._done.wait()

        if self._error:
            raise self._error
        else:
            return self._result

    def __await__(self):
        return self.wait().__await__()

    def set_result(self, result=None):
        if self._done.is_set():
            raise InvalidOperation('Already set')

        self._done.set()
        self._result = result

    def set_error(self, error):
        if self._done.is_set():
            raise InvalidOperation('Already set')

        self._done.set()
        self._error = error

    def isset(self):
        return self._done.is_set()


class UniqueItemMap:
    def __init__(self, capacity):
        self._capacity = capacity
        self._items = {}
        self._freed_ids = deque()
        self._next_free = 0

    def add(self, item):
        if self._freed_ids:
            item_id = self._freed_ids.popleft()
        elif self._next_free < self._capacity:
            item_id = self._next_free
            self._next_free += 1
        else:
            raise QueueFull('Maximum number of IDs used')

        self._items[item_id] = item
        return item_id

    def __delitem__(self, item_id):
        del self._items[item_id]
        if item_id + 1 == self._next_free:
            self._next_free = item_id
        else:
            self._freed_ids.append(item_id)

    def __getitem__(self, item_id):
        return self._items[item_id]

    def __len__(self):
        return len(self._items)

    def __contains__(self, item_id):
        return item_id in self._items
