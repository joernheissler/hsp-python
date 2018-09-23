import trio
import trio.testing


async def _raise_if_blocking():
    await trio.testing.wait_all_tasks_blocked()
    raise trio.WouldBlock()


async def _run_then_cancel(result, cancel_scope, func, args):
    result.append(await func(*args))
    cancel_scope.cancel()


async def _run_nb(func, args):
    async with trio.open_nursery() as nursery:
        nursery.start_soon(_raise_if_blocking)
        result = []
        nursery.start_soon(_run_then_cancel, result, nursery.cancel_scope, func, args)
    return result[0]


def run_nb(func, *args, **kwargs):
    return trio.run(_run_nb, func, args, **kwargs)
