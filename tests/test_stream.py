import pytest

from hsp.stream import write_varint, write_bytearray, BufferedReceiver
from hsp.exception import LimitBreached, IncompleteMessage, CodingError
from run_nb import run_nb
from trio import WouldBlock
from trio.testing import MemoryReceiveStream


def test_write_varint():
    for inp, out in [
        (0, '00'),
        (1, '01'),
        (127, '7f'),
        (128, '80 01'),
        (129, '81 01'),
        (1936442, 'ba 98 76'),
        (165580141, 'ed 9a fa 4e'),
        (4294967295, 'ff ff ff ff 0f'),
    ]:
        buf = bytearray()
        write_varint(buf, inp)
        assert buf == bytes.fromhex(out.replace(' ', ''))


def test_write_bytearray():
    buf = bytearray()
    write_bytearray(buf, b'')
    assert buf == b'\x00'

    buf = bytearray()
    tmp = bytes(i for i in range(256))
    write_bytearray(buf, tmp)
    assert buf == b'\x80\x02' + tmp


def test_buffered_receiver():
    stream = MemoryReceiveStream()
    receiver = BufferedReceiver(stream)

    stream.put_data(b'AB')
    # Receive multiple bytes from stream
    assert run_nb(receiver.receive_byte) == 65
    # Read from internal buffer only
    assert run_nb(receiver.receive_byte) == 66

    # Check if stream is empty
    with pytest.raises(WouldBlock):
        run_nb(receiver.receive_byte)

    stream.put_data(b'FOOBAR')
    assert run_nb(receiver.receive_exactly, 3) == b'FOO'
    assert run_nb(receiver.receive_exactly, 3) == b'BAR'

    # Check if stream is empty
    with pytest.raises(WouldBlock):
        run_nb(receiver.receive_byte)

    stream.put_data(b'\x06FOOBAR\xed\x9a\xfa\x4e')
    stream.put_eof()
    assert run_nb(receiver.receive_bytearray, 32) == b'FOOBAR'
    assert run_nb(receiver.receive_varint, 2**32) == 165580141

    with pytest.raises(EOFError):
        run_nb(receiver.receive_byte)


def test_varint_eof():
    stream = MemoryReceiveStream()
    receiver = BufferedReceiver(stream)

    stream.put_data(b'\xff')
    stream.put_eof()

    with pytest.raises(IncompleteMessage):
        run_nb(receiver.receive_varint, 1024)


def test_varint_trailing_zero():
    stream = MemoryReceiveStream()
    receiver = BufferedReceiver(stream)

    # Correct coding for 1 is "01" in one byte.
    stream.put_data(b'\x81\x00')
    stream.put_eof()

    with pytest.raises(CodingError):
        run_nb(receiver.receive_varint, 1024)


def test_varint_limit_a():
    stream = MemoryReceiveStream()
    receiver = BufferedReceiver(stream)

    stream.put_data(b'\x40')
    stream.put_eof()

    with pytest.raises(LimitBreached):
        run_nb(receiver.receive_varint, 20)


def test_varint_limit_b():
    stream = MemoryReceiveStream()
    receiver = BufferedReceiver(stream)

    stream.put_data(b'\xba\x98\xf6\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80')
    stream.put_eof()

    with pytest.raises(LimitBreached):
        run_nb(receiver.receive_varint, 2**64)
