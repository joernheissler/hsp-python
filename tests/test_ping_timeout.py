import pytest
import trio
import trio.testing
from hsp import HspConnection
from hsp.exception import PingTimeout


async def _test_timeout():
    client_stream, server_stream = trio.testing.memory_stream_pair()
    hsp = HspConnection(client_stream, ping_interval=0.25, ping_timeout=0.25)
    with trio.fail_after(2):
        with pytest.raises(PingTimeout):
            await hsp.run()


def test_timeout():
    trio.run(_test_timeout)
