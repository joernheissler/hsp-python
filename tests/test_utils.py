import pytest
import trio
from hsp.utils import Future

def test_error():
    fut = Future()
    fut.set_error(ValueError('foo'))
    fut.set_error(ValueError('bar'))
    with pytest.raises(ValueError, match='foo'):
        trio.run(fut.wait)


def test_result():
    fut = Future()
    fut.set_result('foo')
    fut.set_result('bar')
    assert trio.run(fut.wait) == 'foo'
