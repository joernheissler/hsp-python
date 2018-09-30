import pytest
import trio
from hsp.utils import Future, UniqueItemMap, Exchange
from hsp.exception import InvalidOperation, QueueFull, DuplicateKeyError
from run_nb import run_nb


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

    foo = object()
    bar = object()
    baz = object()

    assert len(q) == 0
    id0 = q.add(foo)
    id1 = q.add(bar)
    assert len(q) == 2
    id2 = q.add(baz)
    assert q[id1] is bar

    assert len(q.values()) == 3
    assert set(q.values()) == {foo, bar, baz}

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


async def _test_exchange():
    ex = Exchange()

    async def _recv_a(*, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        async with ex.recv('a') as value:
            assert value == 'value a'

    async def _recv_b(*, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        async with ex.recv('b') as value:
            assert value == 'value b'

    async def _recv_b2(*, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        with pytest.raises(DuplicateKeyError):
            async with ex.recv('b') as value:
                assert value == 'value b'

    async def _recv_d(*, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        with trio.open_cancel_scope() as scope:
            scope.cancel()
            async with ex.recv('d') as value:
                assert value == 'value d'

    async def _recv_def(*, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        async with ex.recv_default() as value:
            assert value == 'value c'

    async def _send():
        await ex.send('b', 'value b')
        await ex.send('c', 'value c')
        with trio.open_cancel_scope() as scope:
            scope.cancel()
            await ex.send('a', 'value a')
        with pytest.raises(KeyError):
            await ex.send('d', 'value d')

    async def _recv_ab(*, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        async with ex.recv('a') as value:
            assert value == 'value a'
            # Block forever
            await trio.Event().wait()

    async def _send_a(*, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        await ex.send('a', 'value a')

    async def _send_b(*, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        await ex.send('b', 'value b')

    async with trio.open_nursery() as nursery:
        await nursery.start(_recv_a)
        await nursery.start(_recv_b)
        await nursery.start(_recv_b2)
        await nursery.start(_recv_def)
        await nursery.start(_recv_d)
        nursery.start_soon(_send)

    with pytest.raises(RuntimeError):
        async with trio.open_nursery() as nursery:
            await nursery.start(_recv_ab)
            await nursery.start(_send_a)
            await nursery.start(_send_b)


def test_exchange():
    run_nb(_test_exchange)
