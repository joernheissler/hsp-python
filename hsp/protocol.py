import attr
from abc import ABCMeta, abstractmethod
from typing import Optional
import logging
from .utils import Exchange
from async_generator import async_generator, yield_, asynccontextmanager


class HspData(metaclass=ABCMeta):
    NEED_ACK = False
    MSG_TYPE = None

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


@attr.s(cmp=False)
class HspExchange:
    """
    `recv` and `recv_async` install a request for a specific message type and then
    block until a message with this type is received by `task`.
    As long as the `recv` body is running, `task` stays blocked. After the context manager
    finished, `task` is unblocked, but won't continue before the next trio checkpoint.

    If the `recv` body raises an HspError, this is returned to the sender. Else an ACK is returned.

    To receive message types A and B, the code needs to look like this:
        async with o.recv(A) as msg:
            ...
        # There must be no trio checkpoints here!

        async with o.recv(B) as msg:
            ...

    Or to receive message types C and process them in the background:
        while True:
            await o.recv_async(C, nursery, func, args)
            # Also, no trio checkpoints here!
    """

    hsp = attr.ib()
    _exchange = attr.ib(factory=Exchange, init=False)

    async def task(self):
        async for msg in self.hsp.received_data:
            await self._exchange.send(msg.msg_type, msg)

    @asynccontextmanager
    @async_generator
    async def recv(self, cls):
        async with self._exchange.recv(cls.MSG_TYPE) as msg:
            decoded = cls.decode(msg.payload, msg.msg_id)
            logging.debug('Received protocol message: %s', decoded)
            try:
                await yield_(decoded)
            except HspError as err:
                await msg.send_error(err.ERROR_CODE, err.encoded)
            else:
                await msg.send_ack()

    async def recv_async(self, cls, nursery, func, *args):
        async with self._exchange.recv(cls.MSG_TYPE) as msg:
            decoded = cls.decode(msg.payload, msg.msg_id)
            logging.debug('Received protocol message: %s', decoded)

            async def coro():
                # task_status.started()
                try:
                    await func(decoded, *args)
                except HspError as err:
                    await msg.send_error(err.ERROR_CODE, err.encoded)
                else:
                    await msg.send_ack()

            nursery.start_soon(coro)

    async def send(self, msg: HspData, *, wait: Optional[bool]=None, prio: int=0):
        logging.debug('Sending protocol message: %s', msg)
        result = await self.hsp.send(msg.MSG_TYPE, msg.encoded, msg.NEED_ACK, prio)

        if not (msg.NEED_ACK if wait is None else wait):
            return

        response = await result
        if not response or not response.IS_ERROR:
            return
        elif response.error_code is None:
            raise HspUndefinedError.decode(b'')
        else:
            raise msg.get_error_cls(response.error_code).decode(response.error_data)
