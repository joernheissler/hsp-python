import pytest
import trio
from hsp.utils import Future
from hsp.exception import InvalidOperation


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
