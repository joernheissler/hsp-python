import trio


class Future:
    def __init__(self):
        self._done = trio.Event()
        self._result = None
        self._error = None

    async def wait(self):
        await self._done.wait()

        if self._error:
            raise self._error
        else:
            return self._result

    def __await__(self):
        return self.wait().__await__()

    def set_result(self, result=None):
        if self._done.is_set():
            return

        self._done.set()
        self._result = result

    def set_error(self, error):
        if self._done.is_set():
            return

        self._done.set()
        self._error = error
