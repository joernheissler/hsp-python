import attr
import trio
from outcome import Value
from .exception import InvalidOperation, QueueFull, DuplicateKeyError
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

    def values(self):
        return self._items.values()

_default = object()


@attr.s(cmp=False)
class _Receiver:
    _exchange = attr.ib()
    _key = attr.ib()

    def _abort(self, __):
        del self._exchange._receivers[self._key]
        return trio.hazmat.Abort.SUCCEEDED

    async def __aenter__(self):
        if self._key in self._exchange._receivers:
            raise DuplicateKeyError(self._key)

        self._exchange._receivers[self._key] = trio.hazmat.current_task()
        return await trio.hazmat.wait_task_rescheduled(self._abort)

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._exchange._sender:
            trio.hazmat.reschedule(self._exchange._sender)
            self._exchange._sender = None


@attr.s(cmp=False)
class Exchange:
    _receivers = attr.ib(factory=dict, init=False)
    _sender = attr.ib(default=None, init=False)

    def recv(self, key):
        return _Receiver(self, key)

    def recv_default(self):
        return _Receiver(self, _default)

    def _abort(self, __):
        assert self._sender
        self._sender = None
        return trio.hazmat.Abort.SUCCEEDED

    async def send(self, key, value):
        if self._sender:
            raise RuntimeError('Send already running')
        try:
            receiver = self._receivers.pop(key)
        except KeyError:
            receiver = self._receivers.pop(_default)

        trio.hazmat.reschedule(receiver, Value(value))
        self._sender = trio.hazmat.current_task()
        await trio.hazmat.wait_task_rescheduled(self._abort)
