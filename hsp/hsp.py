import attr
import trio
from enum import IntEnum
from outcome import Value, Error
from queue import deque
from typing import ByteString
from warnings import warn

from . import exception
from .stream import StreamBuffer


class Command(IntEnum):
    DATA = 0
    DATA_ACK = 1
    ACK = 2
    PING = 3
    PONG = 4
    ERROR = 5
    ERROR_UNDEF = 6


@attr.s(cmp=False)
class MessageIdPool:
    _capacity = attr.ib(type=int)
    _items = attr.ib(factory=dict)
    _freed_ids = attr.ib(factory=deque)
    _next_free = 0

    def add(self, item) -> int:
        if self._freed_ids:
            item_id = self._freed_ids.popleft()
        elif self._next_free < self._capacity:
            item_id = self._next_free
            self._next_free += 1
        else:
            raise Exception('Maximum number of IDs used')

        self._items[item_id] = item
        return item_id

    def pop(self, item_id: int):
        item = self._items.pop(item_id)
        if self._next_free == item_id + 1:
            self._next_free = item_id
        else:
            self._freed_ids.append(item_id)
        return item

    @property
    def ids(self):
        return set(self._items.keys())

    def __getitem__(self, item_id: int):
        return self._items[item_id]

    def __len__(self) -> int:
        return len(self._items)


@attr.s(cmp=False)
class Data:
    _hsp = attr.ib(repr=False)
    _msg_id = attr.ib(type=int)
    msg_type = attr.ib(type=int)
    payload = attr.ib(type=ByteString)
    _responded = attr.ib(type=bool, default=False)
    _unblock = attr.ib(factory=trio.Event)

    def __del__(self):
        if not self._responded:
            warn('Not responded to: {}'.format(self))

    def ack(self) -> None:
        if self._responded:
            raise Exception('Already responded')
        self._responded = True

        self._unblock.set()
        if self._msg_id is None:
            return

        self._hsp.stream.send_struct('>BL', Command.ACK, self._msg_id)

    def error(self, err_type: int = None, payload: ByteString = None) -> None:
        if self._responded:
            raise Exception('Already responded')
        self._responded = True

        self._unblock.set()
        if self._msg_id is None:
            return

        if err_type is None:
            self._hsp.stream.send_struct('>BL', Command.ERROR_UNDEF, self._msg_id)
        else:
            self._hsp.stream.send_struct('>BLHL', Command.ERROR, self._msg_id, err_type, len(payload))
            self._hsp.stream.send(payload)

    def unblock(self):
        self._unblock.set()


@attr.s
class HspConnection:
    stream = attr.ib(type=StreamBuffer)

    _nursery = attr.ib()

    # Maximum length of received payload.
    max_data = attr.ib(default=(2 ** 20))

    # Maximum length of error string.
    max_error = attr.ib(default=(2 ** 16))

    # Maximum outgoing message ID.
    max_msg_id = attr.ib(default=(2 ** 16))

    # Task that currently receives data
    _recv_data_task = None

    def __attrs_post_init__(self):
        self._message_ids = MessageIdPool(self.max_msg_id)
        self._pings = deque()
        self._recv_parker = trio.hazmat.ParkingLot()

    async def ping(self) -> None:
        self.stream.send_byte(Command.PING)
        ev = [False, trio.hazmat.current_task()]
        self._pings.append(ev)
        self._recv_parker.unpark_all()

        def abort(__):
            ev[0] = True
            return trio.hazmat.Abort.SUCCEEDED
        await trio.hazmat.wait_task_rescheduled(abort)

    def send(self, data_type: int, payload: ByteString) -> None:
        self.stream.send_struct('>BHL', Command.DATA, data_type, len(payload))
        self.stream.send(payload)

    async def send_ack(self, data_type: int, payload: ByteString) -> None:
        ev = [False, trio.hazmat.current_task()]
        msg_id = self._message_ids.add(ev)
        self.stream.send_struct('>BLHL', Command.DATA_ACK, msg_id, data_type, len(payload))
        self.stream.send(payload)
        self._recv_parker.unpark_all()

        def abort(__):
            ev[0] = True
            return trio.hazmat.Abort.SUCCEEDED
        await trio.hazmat.wait_task_rescheduled(abort)

    async def recv(self) -> Data:
        if self._recv_data_task:
            raise Exception('Already receiving data')

        def abort(__):
            self._recv_data_task = None
            return trio.hazmat.Abort.SUCCEEDED

        self._recv_data_task = trio.hazmat.current_task()
        self._recv_parker.unpark_all()
        return await trio.hazmat.wait_task_rescheduled(abort)

    async def _recv_ping(self) -> None:
        self.stream.send_byte(Command.PONG)

    async def _recv_pong(self) -> None:
        try:
            cancelled, task = self._pings.popleft()
        except IndexError as ex:
            raise exception.UnexpectedPong() from ex
        if not cancelled:
            trio.hazmat.reschedule(task, Value(None))

    async def _recv_data(self) -> None:
        data_type, plen = await self.stream.recv_struct('>HL')
        await self._recv_data_any(None, data_type, plen)

    async def _recv_data_ack(self) -> None:
        msg_id, data_type, plen = await self.stream.recv_struct('>LHL')
        await self._recv_data_any(msg_id, data_type, plen)

    async def _recv_data_any(self, msg_id, data_type, plen):
        if plen >= self.max_data:
            raise exception.LimitBreached('Peer tried to send us {} bytes, limit is {}'.format(plen, self.max_data))

        payload = await self.stream.recv_exactly(plen)
        data = Data(self, msg_id, data_type, payload)
        if not self._recv_data_task:
            raise Exception('Was not expecting any data')
        trio.hazmat.reschedule(self._recv_data_task, Value(data))
        self._recv_data_task = None
        await data._unblock.wait()

    async def _recv_ack(self) -> None:
        msg_id, = await self.stream.recv_struct('>L')
        try:
            cancelled, task = self._message_ids.pop(msg_id)
        except KeyError as ex:
            raise exception.UnexpectedAck(msg_id) from ex
        if not cancelled:
            trio.hazmat.reschedule(task, Value(None))

    async def _recv_error(self) -> None:
        msg_id, error_type, plen = await self.stream.recv_struct('>LHL')
        if plen >= self.max_error:
            raise exception.LimitBreached('Peer tried to send us {} bytes, limit is {}'.format(plen, self.max_error))
        payload = await self.stream.recv_exactly(plen)
        try:
            cancelled, task = self._message_ids.pop(msg_id)
        except KeyError as ex:
            raise exception.UnexpectedAck(msg_id) from ex
        if not cancelled:
            trio.hazmat.reschedule(task, Error(exception.DataError(error_type, payload)))

    async def _recv_error_undef(self) -> None:
        msg_id, = await self.stream.recv_struct('>L')
        try:
            cancelled, task = self._message_ids.pop(msg_id)
        except KeyError as ex:
            raise exception.UnexpectedAck(msg_id) from ex
        if not cancelled:
            trio.hazmat.reschedule(task, Error(exception.DataError(None, None)))

    async def _recv_task(self, *, task_status=trio.TASK_STATUS_IGNORED) -> None:
        handlers = {
            Command.PING: self._recv_ping,
            Command.PONG: self._recv_pong,
            Command.DATA: self._recv_data,
            Command.DATA_ACK: self._recv_data_ack,
            Command.ACK: self._recv_ack,
            Command.ERROR: self._recv_error,
            Command.ERROR_UNDEF: self._recv_error_undef,
        }

        with trio.open_cancel_scope() as self._cancel_recv:
            task_status.started()

            while True:
                if not self._recv_data_task and not self._pings and not len(self._message_ids):
                    await self._recv_parker.park()
                try:
                    cmd = await self.stream.recv_byte()

                    try:
                        handler = handlers[cmd]
                    except KeyError as ex:
                        raise exception.UnknownCommand(cmd) from ex

                    try:
                        await handler()
                    except EOFError as ex:
                        raise exception.IncompleteMessage() from ex
                except trio.Cancelled as ex:
                    raise Exception('Cancelled while receiving!') from ex

    async def __aenter__(self):
        await self._nursery.start(self._recv_task)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._recv_data_task:
            trio.hazmat.reschedule(self._recv_data_task, Error(Exception('Quit Context Manager')))
            self._recv_data_task = None
        while self._pings:
            cancelled, task = self._pings.popleft()
            if not cancelled:
                trio.hazmat.reschedule(task, Error(Exception('Quit Context Manager')))
        for msg_id in self._message_ids.ids:
            cancelled, task = self._message_ids.pop(msg_id)
            if not cancelled:
                trio.hazmat.reschedule(task, Error(Exception('Quit Context Manager')))
        self._cancel_recv.cancel()
