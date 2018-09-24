import pytest
import random

from hsp.queue import SendQueue, PingQueue, DataQueue
from hsp.exception import QueueFull
from run_nb import run_nb
from trio import WouldBlock


def test_send_queue():
    q = SendQueue()
    assert len(q) == 0

    with pytest.raises(WouldBlock):
        run_nb(q.wait_nonempty)

    msg = [object() for __ in range(4)]
    q.put(2, msg[0])
    q.put(2, msg[1])
    q.put(0, msg[2])
    q.put(1, msg[3])
    assert len(q) == 4

    run_nb(q.wait_nonempty)

    # Unqueue everything and check ordering
    x = list(q)
    assert len(q) == 0
    assert(x[0] is msg[2])
    assert(x[1] is msg[3])
    assert(x[2] is msg[0])
    assert(x[3] is msg[1])

    with pytest.raises(WouldBlock):
        run_nb(q.wait_nonempty)


def test_ping_queue():
    q = PingQueue()
    assert len(q) == 0

    with pytest.raises(IndexError):
        q.get()

    with pytest.raises(ValueError):
        q.remove('notexists')

    assert len(q) == 0

    q.add('msg0')
    assert len(q) == 1

    q.remove('msg0')
    assert len(q) == 0

    with pytest.raises(ValueError):
        q.remove('msg0')

    q.add('msg0')
    q.add('msg1')
    q.add('msg2')
    assert len(q) == 3

    q.remove('msg1')
    q.remove('msg2')
    assert len(q) == 1

    q.remove('msg0')
    assert len(q) == 0


def test_data_queue():

    class Message:
        msg_id = None

    msg = [Message() for __ in range(5)]

    q = DataQueue(5)
    assert len(q) == 0

    random.seed(0, 2)

    q.add(msg[0])
    q.add(msg[1])
    assert len(q) == 2

    with pytest.raises(ValueError):
        q.add(msg[1])
    q.add(msg[2])
    q.add(msg[3])
    with pytest.raises(QueueFull):
        q.add(msg[4])
    assert len(q) == 4

    assert q.get(msg[0].msg_id) is msg[0]
    assert len(q) == 4

    assert q.pop(msg[0]) is msg[0]
    assert len(q) == 3

    q.add(msg[4])
