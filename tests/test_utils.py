import pytest
import trio
from hsp.utils import Future, UniqueItemMap
from hsp.exception import InvalidOperation, QueueFull


def test_error():
    fut = Future()
    assert not fut.isset()
    fut.set_error(ValueError('foo'))
    assert fut.isset()
    with pytest.raises(InvalidOperation):
        fut.set_result('bar')
    with pytest.raises(InvalidOperation):
        fut.set_error(ValueError('bar'))
    with pytest.raises(ValueError, match='foo'):
        trio.run(fut.wait)


async def wait(fut):
    # Test Future.__await__. trio.run can't call this directly.
    return await fut


def test_result():
    fut = Future()
    assert not fut.isset()
    fut.set_result('foo')
    assert fut.isset()
    with pytest.raises(InvalidOperation):
        fut.set_error(ValueError('bar'))
    with pytest.raises(InvalidOperation):
        fut.set_result('bar')
    assert trio.run(wait, fut) == 'foo'


def test_item_map():
    q = UniqueItemMap(3)

    bar = object()
    assert len(q) == 0
    id0 = q.add('foo')
    id1 = q.add(bar)
    assert len(q) == 2
    id2 = q.add('baz')
    assert q[id1] is bar

    with pytest.raises(QueueFull):
        id3 = q.add('boo')

    assert len({id0, id1, id2}) == 3
    del q[id1]
    assert len(q) == 2
    id3 = q.add('boo')
    assert len({id0, id2, id3}) == 3
    del q[id0]
    del q[id2]
    assert id3 in q
    del q[id3]
    assert id3 not in q
    assert len(q) == 0
