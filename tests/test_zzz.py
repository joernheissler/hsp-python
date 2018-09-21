import attr
import pytest
import trio
import trio.testing
import json

from hsp import HspConnection
from hsp.protocol import Direction, HspData, HspError, HspProtocol
from hsp.utils import Future


class ExampleProtocol(HspData):
    pass


class SomeError(HspError):
    ERROR_CODE = 12345

    @property
    def encoded(self):
        return b':-('

    @classmethod
    def decode(cls, buf):
        assert buf == b':-('
        return cls()


@attr.s
class SomeMessage(ExampleProtocol):
    NEED_ACK = False
    MSG_TYPE = 17005
    DIRECTION = Direction.BOTH

    foo = attr.ib()
    bar = attr.ib()

    @property
    def encoded(self):
        return json.dumps({
            'foo': self.foo,
            'bar': self.bar,
        }).encode()

    @classmethod
    def decode(cls, buf, msg_id):
        msg = cls(**json.loads(buf.decode()))
        msg.msg_id = msg_id
        return msg


@attr.s
class OtherMessage(ExampleProtocol):
    NEED_ACK = True
    MSG_TYPE = 42
    DIRECTION = Direction.TO_SERVER

    ERRORS = {
        SomeError,
    }

    flag = attr.ib()

    @property
    def encoded(self):
        return b'Flag: ' + [b'False', b'True'][self.flag]

    @classmethod
    def decode(cls, buf, msg_id):
        if buf == b'Flag: True':
            msg = cls(True)
        elif buf == b'Flag: False':
            msg = cls(False)
        msg.msg_id = msg_id
        return msg


@attr.s
class Client:
    stream = attr.ib()

    def __attrs_post_init__(self):
        self.hsp = HspConnection(self.stream)
        self.proto = HspProtocol(self.hsp, ExampleProtocol, self, Direction.TO_CLIENT)

    async def run(self):
        try:
            with trio.fail_after(10) as cancel_scope:
                cancel_scope.shield = True
                await self._run()
        except EOFError:
            pass
        except trio.TooSlowError:
            pass
        except trio.MultiError:
            pass
        except Exception as ex:
            raise

    async def _run(self):
        async with trio.open_nursery() as self.nursery:
            await self.nursery.start(self.hsp.run)
            self.nursery.start_soon(self.proto.recv, self.nursery)
            self.nursery.start_soon(self._task)

    async def _task(self):
        await (await self.hsp.ping())
        self.got_some = Future()
        await self.proto.send(SomeMessage('Hello', 1234))
        msg = await self.got_some
        assert msg.foo == 'Test'
        assert msg.bar == 5678
        await self.proto.send(OtherMessage(True))
        with pytest.raises(SomeError):
            await self.proto.send(OtherMessage(False))
        self.nursery.cancel_scope.cancel()

    @SomeMessage.handler
    async def on_some(self, msg):
        self.got_some.set_result(msg)


@attr.s
class Server:
    stream = attr.ib()

    def __attrs_post_init__(self):
        self.hsp = HspConnection(self.stream)
        self.proto = HspProtocol(self.hsp, ExampleProtocol, self, Direction.TO_SERVER)

    async def run(self):
        try:
            with trio.fail_after(10) as cancel_scope:
                cancel_scope.shield = True
                await self._run()
        except EOFError:
            pass
        except trio.TooSlowError:
            pass
        except trio.MultiError:
            pass
        except Exception as ex:
            raise

    async def _run(self):
        async with trio.open_nursery() as nursery:
            await nursery.start(self.hsp.run)
            nursery.start_soon(self.proto.recv, nursery)

    @SomeMessage.handler
    async def on_some(self, msg):
        assert msg.foo == 'Hello'
        assert msg.bar == 1234
        await self.proto.send(SomeMessage('Test', 5678))

    @OtherMessage.handler
    async def on_other(self, msg):
        if msg.flag:
            return

        return self.other_delayed(msg)

    async def other_delayed(self, msg):
        raise SomeError()


async def bigtest():
    client_stream, server_stream = trio.testing.lockstep_stream_pair()

    client = Client(client_stream)
    server = Server(server_stream)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(client.run)
        nursery.start_soon(server.run)


def test_everything():
    trio.run(bigtest)
