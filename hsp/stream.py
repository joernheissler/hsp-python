import trio

from typing import Union


class BufferedReceiver:
    def __init__(self, stream: trio.abc.ReceiveStream):
        self.stream = stream
        self.buf = bytearray()

    async def receive_exactly(self, size):
        if len(self.buf) >= size:
            await trio.sleep(0)
        else:
            while len(self.buf) < size:
                await self._receive_more():

        result = self.buf[:size]
        del self.buf[:size]
        return result

    async def receive_byte(self):
        if self.buf:
            await trio.sleep(0)
        else:
            await self._receive_more()

        byte = self.buf[0]
        del self.buf[0]
        return byte

    async def _receive_more(self):
        tmp = await self.stream.receive_some(4096)
        if not tmp:
            raise EOFError
        self.buf.extend(tmp)

    async def receive_varint(self, limit):
        shift = 0
        result = 0
        byte = await self.receive_byte()

        while byte >= 0x80:
            result += (byte - 0x80) << shift
            shift += 7

            # 1 << shift protects against long runs of 80 80 80 ... that do not increase result.
            # Whatever would come after those 80's, it would either exceed limit or be a coding error.
            if max(result, 1 << shift) >= limit:
                raise LimitBreached()

            try:
                byte = await self.receive_byte()
            except EOFError as ex:
                raise IncompleteMessage() from ex

        # If the last byte is 0 and it's not the only byte, the encoding is invalid.
        if shift and not byte:
            raise CodingError()

        result += byte << shift
        if result >= limit:
            raise LimitBreached()

        return result

    async def receive_bytearray(self, limit):
        size = await self.receive_varint(limit)
        return await self.receive_exactly(size)


def write_varint(buf, i):
    while i > 127:
        buf.append(128 + (i & 127))
        i >>= 7
    buf.append(i)


def write_bytearray(buf, data):
    write_varint(buf, len(data))
    buf.extend(data)


def attr_varint(limit_var):
    return attr.ib(
        type=int,
        metadata={
            'hsp': True,
            'limit': limit_var,
            'read': BufferedReceiver.receive_varint,
            'write': write_varint,
        },
    )


def attr_bytearray(limit_var):
    return attr.ib(
        type=Union[bytes, bytearray],
        metadata={
            'hsp': True,
            'limit': limit_var,
            'read': BufferedReceiver.receive_bytearray,
            'write': write_bytearray,
        },
    )
