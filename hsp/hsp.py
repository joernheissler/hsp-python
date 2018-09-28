import trio
import attr

from . import messages, queue, exception, utils
from .stream import BufferedReceiver


@attr.s(repr=False, cmp=False)
class HspConnection:
    stream = attr.ib()

    # Maximum value of message type.
    max_type = attr.ib(default=1 << 16)

    # Maximum length of received payload.
    max_data = attr.ib(default=(1 << 20))

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

    # Number of bytes to send out at the same time; small messages are concatenated until the size is reached.
    writer_chunk_size = attr.ib(default=4096)

    # Receive queue size
    receive_queue_size = attr.ib(default=64)

    def __attrs_post_init__(self):
        # Queue of PINGs that are still waiting for a PONG.
        self._ping_queue = queue.PingQueue()

        # Map from msg_id to DataAck messages that are waiting for a reply.
        self._data_queue = utils.UniqueItemMap(self.max_msg_id)

        # Priority queue of outgoing messages  XXX should be bounded to prevent DoS with PINGs or DATA_ACKs.
        self._send_queue = queue.SendQueue()

        self._receiver = BufferedReceiver(self.stream)

        # XXX None to interrupt this? Or change queue impl. to finish the generator.
        self.received_data = trio.Queue(self.receive_queue_size)

    async def run(self, *, task_status=trio.TASK_STATUS_IGNORED):
        try:
            async with self.stream, trio.open_nursery() as nursery:
                self.nursery = nursery
                nursery.start_soon(self._child_ping)
                nursery.start_soon(self._child_recv)
                nursery.start_soon(self._child_send)
                task_status.started()
        except trio.BrokenStreamError as ex:
            raise exception.NetworkError(str(ex)) from ex
        finally:
            pass
            # XXX cleanup

    async def _child_ping(self):
        try:
            while True:
                await trio.sleep(self.ping_interval)
                with trio.fail_after(self.ping_timeout):
                    await (await self.ping())
        except trio.TooSlowError as ex:
            raise exception.PingTimeout() from ex

    async def _child_recv(self):
        types = messages.HspMessage.get_types()
        cmd_limit = max(types) + 1

        while True:
            cmd = await self._receiver.receive_varint(cmd_limit)
            msg = await types[cmd].receive(self)
            await msg.handle()

    async def _child_send(self):
        while True:
            buf = bytearray()

            # Wait until there might be space on the Stream.
            # This gives higher priority messages a chance to get sent earlier.
            await self.stream.wait_send_all_might_not_block()
            await self._send_queue.wait_nonempty()

            for writefunc in self._send_queue:
                writefunc(buf)

                if len(buf) >= self.writer_chunk_size:
                    break

            await self.stream.send_all(buf)

    def close(self):
        self.nursery.cancel_scope.cancel()

    async def send(self, msg_type, payload, req_ack=False, prio=0):
        """
        Enqueue a data message. This may block until the output queue got space.
        XXX Implement this blocking behaviour.
        """
        if req_ack:
            msg = messages.DataAck(self, None, msg_type, payload, prio)
        else:
            msg = messages.Data(self, msg_type, payload, prio)

        return await msg.send()

    async def ping(self):
        return await messages.Ping(self).send()
