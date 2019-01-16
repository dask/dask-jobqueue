#!/usr/bin/env python

import asyncio
import subprocess
import psutil
import time
import sys

from tornado import gen

from dask_jobqueue import DEBUGCluster
from distributed import Client, as_completed, LocalCluster
from distributed.scheduler import KilledWorker


@gen.coroutine
def sleepy():
    yield gen.sleep(1)


def sleep_abit(x):
    time.sleep(1)
    return x+10


def main():
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
                    subprocess.call(f"kill -15 {pid}", shell=True)
    assert count == 10

    c.close()


async def a_c_main():
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
    for ret in work_queue:
        try:
            result = await ret
        except KilledWorker:
            c.retry([ret])
            work_queue.add(ret)
        else:
            print("result", result)
            count += 1
            if count == 3:
                for k, v in c._scheduler_identity["workers"].items():
                    pid = int(v['id'].split('--')[-2])
                    subprocess.call(f"kill -15 {pid}", shell=True)
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
        subprocess.call(f"kill -15 {pid}", shell=True)

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
    if sys.argv[1] == "async_g":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(a_g_main())
    # elif sys.argv[1] == "local":
    #     local_main()
    else:
        main()
