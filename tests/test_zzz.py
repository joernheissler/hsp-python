import attr
import pytest
import trio
import trio.testing
import json

from hsp import HspConnection
from hsp.protocol import HspData, HspError, HspExchange, HspUndefinedError


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
#   DIRECTION = Direction.BOTH

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
#   DIRECTION = Direction.TO_SERVER

    ERRORS = {
        SomeError,
    }

    flag = attr.ib()

    @property
    def encoded(self):
        return 'Flag: {!r}'.format(self.flag).encode()

    @classmethod
    def decode(cls, buf, msg_id):
        if buf == b'Flag: True':
            msg = cls(True)
        elif buf == b'Flag: False':
            msg = cls(False)
        else:
            msg = cls(None)

        msg.msg_id = msg_id
        return msg


@attr.s
class Client:
    stream = attr.ib()

    async def run(self):
        try:
            with trio.fail_after(10) as cancel_scope:
                cancel_scope.shield = True
                await self._run()
        except EOFError:
            pass
#       except trio.TooSlowError:
#           pass
#       except trio.MultiError:
#           pass
#       except Exception as ex:
#           raise

    async def _run(self):
        hsp = HspConnection(self.stream)
        exch = HspExchange(hsp)

        async with trio.open_nursery() as nursery:
            await nursery.start(hsp.run)
            nursery.start_soon(exch.task)

            await (await hsp.ping())
            await exch.send(SomeMessage('Hello', 1234))
            async with exch.recv(SomeMessage) as msg:
                assert msg.foo == 'Test'
                assert msg.bar == 5678
            await exch.send(OtherMessage(True))
            with pytest.raises(SomeError):
                await exch.send(OtherMessage(False))
            with pytest.raises(HspUndefinedError):
                await exch.send(OtherMessage(None))
            nursery.cancel_scope.cancel()


@attr.s
class Server:
    stream = attr.ib()

    async def run(self):
        try:
            with trio.fail_after(10) as cancel_scope:
                cancel_scope.shield = True
                await self._run()
        except EOFError:
            pass
#       except trio.TooSlowError:
#           pass
#       except trio.MultiError:
#           pass
#       except Exception as ex:
#           raise

    async def _run(self):
        hsp = HspConnection(self.stream)
        exch = HspExchange(hsp)

        async with trio.open_nursery() as nursery:
            await nursery.start(hsp.run)
            nursery.start_soon(exch.task)

            async with exch.recv(SomeMessage) as msg:
                assert msg.foo == 'Hello'
                assert msg.bar == 1234
                await exch.send(SomeMessage('Test', 5678))

            async with exch.recv(OtherMessage) as msg:
                assert msg.flag is True

            async def other_delayed(msg):
                assert msg.flag is False
                raise SomeError()

            await exch.recv_async(OtherMessage, nursery, other_delayed)

            async with exch.recv(OtherMessage) as msg:
                assert msg.flag is None
                raise HspUndefinedError()


async def tcp_socketpair():
    listeners = await trio.open_tcp_listeners(port=0, host='127.0.0.1')
    assert len(listeners) == 1
    async with listeners[0] as listener:
        return await trio.testing.open_stream_to_socket_listener(listener), await listener.accept()


async def bigtest():
    client_stream, server_stream = trio.testing.lockstep_stream_pair()
    # client_stream, server_stream = await tcp_socketpair()

    client = Client(client_stream)
    server = Server(server_stream)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(client.run)
        nursery.start_soon(server.run)


def test_everything():
    trio.run(bigtest)
