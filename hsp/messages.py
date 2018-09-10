import attr
from .stream import attr_varint, attr_bytearray, write_varint


@attr.s(cmp=False)
class HspMessage:
    NEED_RESP = False
    prio = 0

    hsp = attr.ib()

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
        return cls(hsp, *[
            await recvfunc(hsp.receiver, getattr(hsp, limit_var))
            for __, limit_var, recvfunc, __ in cls._fields()
        ])

    def write(self, buf):
        if self.cancelled:
            return

        write_varint(buf, self.CMD)
        for name, __, __, writefunc in self._fields():
            writefunc(buf, getattr(self, name))

        self._written.set()

    def send(self):
        self.register()
        self._response = None
        self.hsp._send_queue.put_nowait((self.PRIO, self.prio), self)
        self._written = trio.Event()
        if self.NEED_RESP:
            self._responded = trio.Event()
        self._sent = trio.Event()
        self.hsp.nursery.start_soon(self._send_task)
        return self

    async def _send_task(self):
        # XXX fix cancellation
        try:
            with trio.open_cancel_scope() as cancel_scope:
                self._cancel_send = cancel_scope
                await self._written.wait()
            if self.NEED_RESP:
                await self._responded.wait()
        except trio.Cancelled:
            # Don't shut down the nursery!
            pass
        finally:
            self.unregister()
            self._sent.set()

    async def wait_sent(self):
        # XXX fix cancellation
        try:
            await self._sent.wait()
        except trio.Cancelled:
            self.cancel_scope.cancel()
        return self._response

    def set_response(self, msg):
        self._response = msg
        self._responded.set()

    def register(self):
        """
        Overwritten by child classes to somehow register themselves.
        """

    def unregister(self):
        """
        Overwritten by child classes to somehow unregister themselves.
        """


@attr.s(cmp=False)
class Data(HspMessage):
    CMD = 0
    PRIO = 2

    msg_id = None
    msg_type = attr_varint('max_type')
    payload = attr_bytearray('max_data')
    prio = attr.ib(default=0)

    def handle(self, msg):
        self.hsp.nursery.start_soon(self.recv_task, msg)

    async def recv_task(self, msg):
        if self.hsp.on_data:
            await self.hsp.on_data(msg)


@attr.s(cmp=False)
class DataAck(HspMessage):
    CMD = 1
    PRIO = 2
    NEED_RESP = True

    msg_id = attr_varint('max_msg_id')
    msg_type = attr_varint('max_type')
    payload = attr_bytearray('max_data')
    prio = attr.ib(default=0)

    def register(self):
        self.hsp._data_queue.add(self)

    def unregister(self):
        self.hsp._data_queue.pop(self)

    def handle(self, msg):
        self.hsp.nursery.start_soon(self.recv_task, msg)

    async def recv_task(self, msg):
        if self.hsp.on_data:
            await self.hsp.on_data(msg)

        Ack(self.hsp, msg.msg_id).send()


@attr.s(cmp=False)
class Ack(HspMessage):
    CMD = 2
    PRIO = 1

    msg_id = attr_varint('max_msg_id')

    def handle(self):
        self.hsp._data_queue.get(msg.msg_id).set_response(msg)


@attr.s(cmp=False)
class Error(HspMessage):
    CMD = 3
    PRIO = 1

    msg_id = attr_varint('max_msg_id')
    error_code = attr_varint('max_error_code')
    error = attr_bytearray('max_error_length')

    def handle(self):
        self.hsp._data_queue.get(msg.msg_id).set_response(msg)


@attr.s(cmp=False)
class Ping(HspMessage):
    CMD = 4
    PRIO = 0
    NEED_RESP = True

    def register(self):
        self.hsp._ping_queue.add(self)

    def unregister(self):
        self.hsp._ping_queue.pop(self)

    def handle(self, msg):
        self.hsp.nursery.start_soon(self.recv_task, msg)

    async def recv_task(self, msg):
        if self.hsp.on_ping:
            await self.hsp.on_ping(msg)

        messages.Pong(self.hsp).send()


@attr.s(cmp=False)
class Pong(HspMessage):
    CMD = 5
    PRIO = 0

    def handle(self, msg):
        self.hsp._ping_queue.get().set_response(msg)


@attr.s(cmp=False)
class ErrorUndef(HspMessage):
    CMD = 6
    PRIO = 1

    msg_id = attr_varint('max_msg_id')

    def handle(self, msg):
        self.hsp._data_queue.get(msg.msg_id).set_response(msg)
