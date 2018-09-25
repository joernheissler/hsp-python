import pytest
import trio
import trio.testing
from hsp import HspConnection
from hsp.exception import UnexpectedAck
from hsp.messages import Ack


async def _test_bad_ack():
    client_stream, server_stream = trio.testing.memory_stream_pair()
    await server_stream.send_all(bytes([Ack.CMD, 42]))
    hsp = HspConnection(client_stream)
    with trio.fail_after(2):
        with pytest.raises(UnexpectedAck):
            await hsp.run()


def test_bad_ack():
    trio.run(_test_bad_ack)
