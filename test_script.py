#!/usr/bin/env python

import asyncio
import subprocess
import psutil
import time
import sys
import typing

from tornado import gen

from dask_jobqueue import DEBUGCluster
from distributed import Client, as_completed, LocalCluster
from distributed.scheduler import KilledWorker


class possible_async_as_completed:
    """
    I'm now not convinced this is necessary and was probably a waste of time :(
    """
    def __init__(self, futures: typing.List['future or task'],
                 done: asyncio.Queue = None, todo: set = None,
                 loop: 'loop' = None, timeout: int = None):
        self.futures = futures
        self.done = asyncio.Queue()
        self.todo = set()
        self.loop = loop
        self.timeout = timeout

    def add(self, waitable):
        self.todo.add(asyncio.ensure_future(waitable))

    # This is *not* a @coroutine!  It is just an iterator (yielding Futures).
    def __iter__(self):
        """Return an iterator whose values are coroutines.

        When waiting for the yielded coroutines you'll get the results (or
        exceptions!) of the original Futures (or coroutines), in the order
        in which and as soon as they complete.

        This differs from PEP 3148; the proper way to use this is:

            for f in as_completed(fs):
                result = await f  # The 'await' may raise.
                # Use result.

        If a timeout is specified, the 'await' will raise
        TimeoutError when the timeout occurs before all Futures are done.

        Note: The futures 'f' are not necessarily members of fs.
        """
        # if futures.isfuture(fs) or coroutines.iscoroutine(fs):
        #     raise TypeError(f"expect a list of futures, not {type(fs).__name__}")
        loop = self.loop if self.loop is not None else asyncio.get_event_loop()
        self.todo = {asyncio.ensure_future(f, loop=loop) for f in set(self.futures)}

        timeout_handle = None

        def _on_timeout():
            for f in self.todo:
                f.remove_done_callback(_on_completion)
                self.done.put_nowait(None)  # Queue a dummy value for _wait_for_one().
            self.todo.clear()  # Can't do todo.remove(f) in the loop.

        def _on_completion(f):
            if not self.todo:
                return  # _on_timeout() was here first.
            self.todo.remove(f)
            self.done.put_nowait(f)
            if not self.todo and timeout_handle is not None:
                timeout_handle.cancel()

        for f in self.todo:
            f.add_done_callback(_on_completion)
        if self.todo and self.timeout is not None:
            timeout_handle = loop.call_later(self.timeout, _on_timeout)
        return self

    async def _wait_for_one(self):
        f = await self.done.get()
        self.done.task_done()
        if f is None:
            # Dummy value from _on_timeout().
            raise TimeoutError
        # result = f.result()
        try:
            return f.result()  # May raise f.exception().
        except Exception:
            # TODO here we should be smart about killed workers or cancelled coroutines
            raise
            # print("FOUND EXCEPTIONSSSS")
            # raise RuntimeError("DYINGDDD")
            # pdb.set_trace()

    def __next__(self):
        if len(self.todo) or self.done.qsize():
            return self._wait_for_one()
        else:
            raise StopIteration


@gen.coroutine
def sleepy():
    yield gen.sleep(1)


def sleep_abit(x):
    time.sleep(1)
    return x+10


def main():
    """
    this function currently doesn't work
    """
    d = DEBUGCluster(cores=1, memory="1gb", extra=["--no-nanny", "--no-bokeh"])
    # d.adapt(minimum=3, maximum=3)
    d.scale(3)
    c = Client(d)

    while len(c._scheduler_identity["workers"]) < 3:
        sleepy()

    print("finally")
    print(c._scheduler_identity)

    ret = c.map(lambda x: sleep_abit(x), list(range(10)))
    # proc = psutil.Process().pid
    print("THIS IS", psutil.Process())

    count = 0
    subprocess.run("ps -u $USER", shell=True)
    work_queue = as_completed(ret)
    for ret in work_queue:
        try:
            result = ret.result()
        except KilledWorker:
            c.retry([ret])
            work_queue.add(ret)
        else:
            print("result", result)
            count += 1
            if count == 3:
                for k, v in c._scheduler_identity["workers"].items():
                    pid = int(v['id'].split('--')[-2])
                    subprocess.call("kill -15 {}".format(pid), shell=True)
        # result = ret.result()
        # print("result", result)
        # count += 1
        # if count == 3:
        #     for k, v in c._scheduler_identity["workers"].items():
        #         pid = int(v['id'].split('--')[-2])
        #         subprocess.call("kill -15 {}".format(pid), shell=True)
    assert count == 10

    c.close()


async def a_c_main():
    """I think this currently works... but i'm not convinced it's bulletproof
    """
    d = DEBUGCluster(cores=1, memory="1gb", extra=["--no-nanny", "--no-bokeh"])
    d.adapt(minimum=3, maximum=3)
    # d.scale(3)
    c = await Client(d, asynchronous=True)

    while len(c._scheduler_identity["workers"]) < 3:
        await asyncio.sleep(1)

    print("finally")
    print(c._scheduler_identity)

    ret = c.map(lambda x: sleep_abit(x), list(range(10)))
    # proc = psutil.Process().pid
    print("THIS IS", psutil.Process())

    count = 0
    subprocess.run("ps -u $USER", shell=True)
    work_queue = as_completed(ret)
    # you must use async for for async functions
    async for ret in work_queue:
        result = await ret
        print("result", result)
        count += 1
        if count == 3:
            for k, v in c._scheduler_identity["workers"].items():
                pid = int(v['id'].split('--')[-2])
                subprocess.call("kill -15 {}".format(pid), shell=True)
                print("killing worker", pid)
        # try:
        #     result = await ret
        # except KilledWorker:
        #     c.retry([ret])
        #     work_queue.add(ret)
        # else:
        #     print("result", result)
        #     count += 1
        #     if count == 3:
        #         for k, v in c._scheduler_identity["workers"].items():
        #             pid = int(v['id'].split('--')[-2])
        #             subprocess.call("kill -15 {}".format(pid), shell=True)
        #             print("killing worker", pid)
    assert count == 10

    c.close()


async def a_g_main():
    d = DEBUGCluster(cores=1, memory="1gb", extra=["--no-nanny", "--no-bokeh"])
    d.adapt(minimum=3, maximum=3)
    # d.scale(3)
    c = await Client(d, asynchronous=True)

    while len(c._scheduler_identity["workers"]) < 3:
        await asyncio.sleep(1)

    print("finally")
    print(c._scheduler_identity)

    ret = c.map(lambda x: sleep_abit(x), list(range(10)))
    # proc = psutil.Process().pid
    print("THIS IS", psutil.Process())

    subprocess.run("ps -u $USER", shell=True)
    gather_task = asyncio.ensure_future(asyncio.gather(*ret))
    await asyncio.sleep(1.2)

    for k, v in c._scheduler_identity["workers"].items():
        pid = int(v['id'].split('--')[-2])
        print("killing pid", pid)
        subprocess.call("kill -15 {}".format(pid), shell=True)

    await asyncio.sleep(3)

    print(c._scheduler_identity)

    ret = await gather_task
    print(ret)

    c.close()


# not sure
# def local_main():
#     d = LocalCluster(ncores=1, n_workers=1)
#     d.adapt(minimum=3, maximum=3)
#     # d.scale(3)
#     c = Client(d)

#     while len(c._scheduler_identity["workers"]) < 3:
#         sleepy()

#     print("finally")
#     print(c._scheduler_identity)

#     ret = c.map(lambda x: sleep_abit(x), list(range(10)))
#     # proc = psutil.Process().pid
#     print("THIS IS", psutil.Process())

#     count = 0
#     subprocess.run("ps -u $USER", shell=True)
#     subprocess.run("pstree $USER -acp", shell=True)
#     work_queue = as_completed(ret)
#     for ret in work_queue:
#         try:
#             result = ret.result()
#         except KilledWorker:
#             c.retry([ret])
#             work_queue.add(ret)
#         else:
#             print("result", result)
#             count += 1
#             if count == 3:
#                 for k, v in c._scheduler_identity["workers"].items():
#                     print(v)
#                     pid = int(v['name'])
#                     subprocess.call(f"kill -15 {pid}", shell=True)
#     assert count == 10

#     c.close()


if __name__ == "__main__":
    if sys.argv[1] == "async_a_c":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(a_c_main())
    elif sys.argv[1] == "async_g":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(a_g_main())
    # elif sys.argv[1] == "local":
    #     local_main()
    else:
        main()
