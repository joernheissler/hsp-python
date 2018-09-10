import trio
import attr
from random import randint

from . import messages, queue

class HspClosed(Exception):
    pass


@attr.s(repr=False, cmp=False)
class HspConnection:
    stream = attr.ib()

    # Called for each PING command with the message as argument.
    # Must be an async function, or None.
    # If it raises any exception, the connection is terminated.
    on_ping = attr.ib(default=None)

    # Called for each DATA or DATA_ACK command with the message as argument.
    # Must be an async function, or None.
    #  - DataError: Send an ERROR or ERROR_UNDEF.
    #  - Other Exceptions: Terminate the connection
    on_data = attr.ib(default=None)

    # Maximum value of message type.
    max_type = attr.ib(default=1 << 16)

    # Maximum length of received payload.
    max_data = attr.ib(default=(1 << 24))

    # Maximum message ID.
    max_msg_id = attr.ib(default=(1 << 128))

    # Maximum error code:
    max_error_code = attr.ib(default=(1 << 32))

    # Maximum length of error string.
    max_error_length = attr.ib(default=(1 << 16))

    # Delay after a PONG was received before next PING is sent.
    ping_interval = attr.ib(default=30)

    # Timeout waiting for PONG before connection is terminated.
    ping_timeout = attr.ib(default=120)

    # Number of bytes to send out at the same time; smaller messages are concatenated, larger messages are split.
    writer_chunk_size = attr.ib(default=4096)

    def __attrs_post_init__(self):
        # Queue of PINGs that are still waiting for a PONG.
        self._ping_queue = queue.PingQueue()

        # Map from msg_id to DataAck messages that are waiting for a reply.
        self._data_queue = queue.DataQueue(self.max_msg_id)

        # Queue of outgoing messages
        self._send_queue = queue.SendQueue()

    async def run(self):
        try:
            async with self.stream, trio.open_nursery() as nursery:
                self.nursery = nursery
                nursery.start_soon(self._child_ping)
                nursery.start_soon(self._child_recv)
                nursery.start_soon(self._child_send)
        finally:
            pass

    async def _child_ping(self):
        while True:
            await trio.sleep(self.ping_interval)
            with trio.fail_after(self.ping_timeout):
                await self.ping().wait_sent()

    async def _child_recv(self):
        types = messages.HspMessage.get_types()
        cmd_limit = max(types) + 1

        while True:
            cmd = await self.receiver.receive_varint(cmd_limit)
            msg = await types[cmd].receive(self)
            msg.handle()

    async def _child_send(self):
        quit = False

        while not quit:
            buf = bytearray()

            # Wait until there might be space on the Stream.
            # This gives higher priority messages a chance to get sent earlier.
            await self.stream.wait_send_all_might_not_block()

            while not buf:
                msg = await self._send_queue.get()
                if not msg:
                    break
                msg.write(buf)

            while len(buf) < self.writer_chunk_size:
                try:
                    msg = self._send_queue.get_nowait()
                except trio.WouldBlock:
                    break

                if not msg:
                    quit = True
                    break

                msg.write(buf)

            await self.stream.write_all(buf)

        # Shuts down the nursery and closes the stream.
        # XXX cancel the nursery?
        raise HspClosed()

    def disconnect(self):
        self._send_queue.put_nowait(None)

    def close(self):
        self.nursery.cancel_scope.cancel()

    def send(self, msg_type, payload, req_ack=False, prio=0):
        if req_ack:
            return messages.DataAck(self, None, msg_type, payload, prio).send()
        else:
            return messages.Data(self, msg_type, payload, prio).send()

    def ping(self):
        return messages.Ping(self).send()
