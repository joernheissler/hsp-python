import attr
import trio
import struct
from typing import ByteString, Tuple


@attr.s(cmp=False)
class StreamBuffer(trio.abc.AsyncResource):
    _stream = attr.ib(type=trio.abc.Stream)
    _nursery = attr.ib()

    _recv_buf = attr.ib(factory=bytearray)

    _send_buf = attr.ib(factory=bytearray)
    _send_eof = False
    _send_parker = attr.ib(factory=trio.hazmat.ParkingLot)
    _send_task_finished = attr.ib(factory=trio.Event)

    def send(self, data: ByteString) -> None:
        self._send_buf.extend(data)
        self._send_parker.unpark_all()

    def send_byte(self, value: int) -> None:
        self._send_buf.append(value)
        self._send_parker.unpark_all()

    def send_struct(self, fmt, *values) -> None:
        self._send_buf.extend(struct.pack(fmt, *values))
        self._send_parker.unpark_all()

    def set_eof(self) -> None:
        self._send_eof = True
        self._send_parker.unpark_all()

    async def recv_exactly(self, size: int) -> bytearray:
        while len(self._recv_buf) < size:
            await self._recv_some()
        result = self._recv_buf[:size]
        del self._recv_buf[:size]
        return result

    async def recv_byte(self) -> int:
        if not self._recv_buf:
            await self._recv_some()
        result = self._recv_buf[0]
        del self._recv_buf[0]
        return result

    async def recv_struct(self, fmt) -> Tuple:
        size = struct.calcsize(fmt)
        buf = await self.recv_exactly(size)
        return struct.unpack(fmt, buf)

    async def _recv_some(self):
        tmp = await self._stream.receive_some(2 ** 16)
        if not tmp:
            raise EOFError
        self._recv_buf.extend(tmp)

    async def _send_task(self, *, task_status=trio.TASK_STATUS_IGNORED) -> None:
        try:
            with trio.open_cancel_scope() as self._cancel_send:
                task_status.started()
                while not self._send_eof or self._send_buf:
                    await self._stream.wait_send_all_might_not_block()
                    if not self._send_buf:
                        await self._send_parker.park()
                    sb = self._send_buf
                    self._send_buf = bytearray()
                    await self._stream.send_all(sb)
        finally:
            self._send_task_finished.set()

    async def aclose(self):
        await self._stream.aclose()

    async def __aenter__(self):
        await self._nursery.start(self._send_task)
        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc, tb):
        self.set_eof()
        with trio.move_on_after(30):
            await self._send_task_finished.wait()
            await self.aclose()
        self._cancel_send.cancel()
        return await super().__aexit__(exc_type, exc, tb)
