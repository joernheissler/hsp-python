import pytest
import trio
import trio.testing
from hsp import HspConnection
from hsp.exception import UnexpectedPong
from hsp.messages import Pong


async def _test_timeout():
    client_stream, server_stream = trio.testing.memory_stream_pair()
    await server_stream.send_all(bytes([Pong.CMD]))
    hsp = HspConnection(client_stream)
    with trio.fail_after(2):
        with pytest.raises(UnexpectedPong):
            await hsp.run()


def test_timeout():
    trio.run(_test_timeout)
