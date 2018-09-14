import outcome
import trio
import attr
from .stream import attr_varint, attr_bytearray, write_varint
from .exception import UnexpectedPong, UnexpectedAck, IncompleteMessage, DataError


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
        if self._written.is_set():
            raise Exception('Already written')
        self._written.set()

        write_varint(buf, self.CMD)
        for name, __, __, writefunc in self._fields():
            writefunc(buf, getattr(self, name))

    async def send(self, task_status=trio.TASK_STATUS_IGNORED):
        if hasattr(self, '_sent'):
            raise Exception('Already sent')

        self._sent = trio.Event()
        self._written = trio.Event()
        self._responded = trio.Event()

        self._prepare_send()
        try:
            await self.hsp.nursery.start(self._send_task)
            self.hsp._send_queue.put((self.PRIO, self.prio), self._write)
            task_status.started()
            await self._sent.wait()
            return self._response.unwrap()
        finally:
            self._finish_send()

    async def _send_task(self, *, task_status=trio.TASK_STATUS_IGNORED):
        try:
            task_status.started()
            await self._written.wait()
            if self.NEED_RESP:
                await self._responded.wait()
            else:
                self._response = outcome.Value(None)
        except trio.Cancelled as ex:
            self._response = outcome.Error(ex)
            raise
        finally:
            self._sent.set()

    def _set_response(self, response):
        self._response = response
        self._responded.set()

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
        self.hsp._data_queue.add(self)

    def _finish_send(self):
        self.hsp._data_queue.pop(self)

    async def handle(self):
        await self.hsp.received_data.put(self)

    async def send_ack(self):
        return await Ack(self.hsp, self.msg_id).send()

    async def send_error(self, error):
        if error.error_code is None:
            await ErrorUndef(self.hsp, self.msg_id).send()
        else:
            await Error(self.hsp, self.msg_id, error.error_code, error.error).send()


@attr.s(cmp=False)
class Ack(HspMessage):
    CMD = 2
    PRIO = 1

    msg_id = attr_varint('max_msg_id')

    async def handle(self):
        try:
            msg = self.hsp._data_queue.get(self.msg_id)
        except KeyError as ex:
            raise UnexpectedAck(self.msg_id) from ex
        msg._set_response(outcome.Value(None))


@attr.s(cmp=False)
class Error(HspMessage):
    CMD = 3
    PRIO = 1

    msg_id = attr_varint('max_msg_id')
    error_code = attr_varint('max_error_code')
    error = attr_bytearray('max_error_length')

    async def handle(self):
        try:
            msg = self.hsp._data_queue.get(self.msg_id)
        except KeyError as ex:
            raise UnexpectedAck(self.msg_id) from ex
        msg._set_response(outcome.Error(DataError(self.error_code, self.error)))


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
        await self.hsp.nursery.start(self._recv_task)

    async def _recv_task(self, *, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        if self.hsp.on_ping:
            await self.hsp.on_ping()

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

        msg._set_response(outcome.Value(None))


@attr.s(cmp=False)
class ErrorUndef(HspMessage):
    CMD = 6
    PRIO = 1

    msg_id = attr_varint('max_msg_id')

    async def handle(self):
        try:
            msg = self.hsp._data_queue.get(self.msg_id)
        except KeyError as ex:
            raise UnexpectedAck(self.msg_id) from ex

        msg._set_response(outcome.Error(DataError()))
