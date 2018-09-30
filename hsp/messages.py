import attr
import trio
from .stream import attr_varint, attr_bytearray, write_varint
from .exception import UnexpectedPong, UnexpectedAck, IncompleteMessage
from .utils import Future

from typing import Optional, Union


@attr.s(cmp=False)
class HspMessage:
    NEED_RESP = False
    prio = 0

    hsp = attr.ib()

    _sender = None

    @classmethod
    def get_types(cls):
        return {
            sub.CMD: sub
            for sub in cls.__subclasses__()
        }

    @classmethod
    def _fields(cls):
        """
        List of message fields. The list is stored in the class for caching.
        Each field is a tuple:
            * name: Name of the attribute in each instance.
            * limit: Maximum size or value
            * read function: Function that reads this field from the stream
            * write function: Function that writes this field to the stream
        """
        try:
            return cls._cached_fields
        except AttributeError:
            pass

        cls._cached_fields = [
            (field.name, field.metadata['limit'], field.metadata['read'], field.metadata['write'])
            for field in attr.fields(cls)
            if field.metadata.get('hsp', False)
        ]

        return cls._cached_fields

    @classmethod
    async def receive(cls, hsp):
        try:
            return cls(hsp, *[
                await recvfunc(hsp._receiver, getattr(hsp, limit_var))
                for __, limit_var, recvfunc, __ in cls._fields()
            ])
        except EOFError as ex:
            raise IncompleteMessage() from ex

    def _write(self, buf):
        write_varint(buf, self.CMD)
        for name, __, __, writefunc in self._fields():
            writefunc(buf, getattr(self, name))
        self._written.set_result()
        if not self.NEED_RESP:
            self._send_result.set_result()

    async def send(self):
        if hasattr(self, '_send_result'):
            raise RuntimeError('Already sent')

        self._send_result = Future()
        self._written = Future()
        await self.hsp.nursery.start(self._send_task)
        return self._send_result

    async def _send_task(self, *, task_status=trio.TASK_STATUS_IGNORED):
        self._prepare_send()
        try:
            self.hsp._send_queue.put((self.PRIO, self.prio), self._write)
            task_status.started()
            await self._written
            await self._send_result
        # except BaseException as ex:
        #     self._send_result.set_error(ex)
        finally:
            self._finish_send()

    def _set_response(self, response):
        self._send_result.set_result(response)

    def _prepare_send(self):
        """Overwritten by child classes to somehow register themselves."""

    def _finish_send(self):
        """Overwritten by child classes to somehow finish themselves."""


@attr.s(cmp=False)
class Data(HspMessage):
    CMD = 0
    PRIO = 2

    msg_id = None
    msg_type = attr_varint('max_type')
    payload = attr_bytearray('max_data')
    prio = attr.ib(default=0)

    async def handle(self):
        await self.hsp.received_data.put(self)

    async def send_ack(self):
        pass

    async def send_error(self, error):
        pass


@attr.s(cmp=False)
class DataAck(HspMessage):
    CMD = 1
    PRIO = 2
    NEED_RESP = True

    msg_id = attr_varint('max_msg_id')
    msg_type = attr_varint('max_type')
    payload = attr_bytearray('max_data')
    prio = attr.ib(default=0)

    def _prepare_send(self):
        if self.msg_id is not None:
            raise RuntimeError('Cannot add a message with a message ID')
        self.msg_id = self.hsp._data_queue.add(self)

    def _finish_send(self):
        del self.hsp._data_queue[self.msg_id]

    async def handle(self):
        await self.hsp.received_data.put(self)

    async def send_ack(self):
        return await Ack(self.hsp, self.msg_id).send()

    async def send_error(self, error_code: Optional[int]=None, error_data: Optional[Union[bytes, bytearray]]=None):
        if error_code is None:
            return await ErrorUndef(self.hsp, self.msg_id).send()
        else:
            return await Error(self.hsp, self.msg_id, error_code, error_data).send()


@attr.s(cmp=False)
class Ping(HspMessage):
    CMD = 4
    PRIO = 0
    NEED_RESP = True

    def _prepare_send(self):
        self.hsp._ping_queue.add(self)

    def _finish_send(self):
        self.hsp._ping_queue.remove(self)

    async def handle(self):
        await Pong(self.hsp).send()


@attr.s(cmp=False)
class Pong(HspMessage):
    CMD = 5
    PRIO = 0

    async def handle(self):
        try:
            msg = self.hsp._ping_queue.get()
        except IndexError as ex:
            raise UnexpectedPong() from ex

        msg._set_response(None)


@attr.s(cmp=False)
class DataResp:
    PRIO = 1
    msg_id = attr_varint('max_msg_id')

    async def handle(self):
        try:
            msg = self.hsp._data_queue[self.msg_id]
        except KeyError as ex:
            raise UnexpectedAck(self.msg_id) from ex

        msg._set_response(self)


@attr.s(cmp=False)
class Ack(HspMessage, DataResp):
    CMD = 2
    IS_ERROR = False


@attr.s(cmp=False)
class Error(HspMessage, DataResp):
    CMD = 3
    IS_ERROR = True
    error_code = attr_varint('max_error_code')
    error_data = attr_bytearray('max_error_length')


@attr.s(cmp=False)
class ErrorUndef(HspMessage, DataResp):
    CMD = 6
    IS_ERROR = True
    error_code = None
    error_data = None
