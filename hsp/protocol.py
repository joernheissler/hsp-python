import attr
from abc import ABCMeta, abstractmethod
from typing import Optional
from enum import IntEnum
import inspect


class Direction(IntEnum):
    TO_CLIENT = 1
    TO_SERVER = 2
    BOTH = 3


class HspData(metaclass=ABCMeta):
    NEED_ACK = False
    MSG_TYPE = None
    DIRECTION = None

    # Iterable of HspError classes that may be returned for this message type
    ERRORS = set()

    def get_error_cls(self, error_code):
        try:
            errors = self._errors
        except AttributeError:
            errors = self._errors = {
                err.ERROR_CODE: err
                for err in self.ERRORS
            }

        return errors[error_code]

    @property
    @abstractmethod
    def encoded(self) -> bytes:
        """
        """

    @classmethod
    @abstractmethod
    def decode(cls, buf: bytearray, msg_id: Optional[int]=None) -> "HspData":
        """
        """

    @classmethod
    def handler(cls, func):
        """
        Function decorator to register handler functions for specific message types.

        @MessageType.handler
        def handle(self, msg):
            pass
        """
        func._hsp_data_class = cls
        return func


class HspError(Exception, metaclass=ABCMeta):
    ERROR_CODE = None

    @property
    @abstractmethod
    def encoded(self) -> bytes:
        """
        """

    @classmethod
    @abstractmethod
    def decode(cls, buf: bytearray) -> "HspError":
        """
        """


class HspUndefinedError(HspError):
    @property
    def encoded(self) -> bytes:
        return b''

    @classmethod
    def decode(cls, buf):
        return cls()


@attr.s
class HspProtocol:
    hsp = attr.ib()
    messages = attr.ib(type=HspData)  # XXX allow multiple classes
    handler = attr.ib(type=object)
    direction = attr.ib(type=Direction)

    # XXX API to change the handlers?
    # And clean up that horrible code a bit!!
    def __attrs_post_init__(self):
        subs = {
            sub
            for sub in self.messages.__subclasses__()
            if sub.DIRECTION & self.direction
        }

        self._types = {}

        for name, member in inspect.getmembers(self.handler):
            cls = getattr(member, '_hsp_data_class', None)

            if cls not in subs:
                continue

            if cls.MSG_TYPE in self._types:
                raise Exception('Duplicate message type {} ({}/{}, {}/{})'.format(
                    cls.MSG_TYPE, cls, member, *self._types[cls.MSG_TYPE]))

            self._types[cls.MSG_TYPE] = cls, member

        subs -= {cls for cls, __ in self._types.values()}

        if subs:
            raise Exception('Unhandled message types: {}'.format(subs))

    # XXX still need priorities. ACK/PING/PONG/ERROR must come before all DATA(_ACK).
    # Some DATA are more important than others, especially on multiplexed connections.
    # For Multiplex, maybe create scheduler with one queue per logical connection and round robin on all queues?
    async def send(self, msg: HspData, wait: Optional[bool]=None):
        result = await self.hsp.send(msg.MSG_TYPE, msg.encoded, msg.NEED_ACK)

        if not (msg.NEED_ACK if wait is None else wait):
            return

        error = await result
        if not error:
            return
        elif error.error_code is None:
            raise HspUndefinedError.decode(b'')
        else:
            raise msg.get_error_cls(error.error_code).decode(error.error_data)

    async def recv(self, nursery):
        async for msg in self.hsp.received_data:
            cls, cb = self._types[msg.msg_type]
            try:
                coro = await cb(cls.decode(msg.payload, msg.msg_id))
                if coro is None:
                    msg.send_ack()
                elif inspect.iscoroutine(coro):
                    nursery.start_soon(self._delayed_recv, msg, coro)
                else:
                    raise TypeError(type(coro))
            except HspError as err:
                msg.send_error(err.ERROR_CODE, err.encoded)

    async def _delayed_recv(self, msg, coro):
        try:
            await coro
            msg.send_ack()
        except HspError as err:
            msg.send_error(err.ERROR_CODE, err.encoded)
