import pytest

from hsp.queue import SendQueue, PingQueue, DataQueue


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
    assert len(q) == 0

    q.add('msg0')
    assert len(q) == 1

    q.add('msg1')
    assert len(q) == 2

    q.add('msg2')
    assert len(q) == 3

    q.remove('msg1')
    q.remove('msg2')
    assert len(q) == 1
    q.remove('msg0')
    assert len(q) == 0
