from __future__ import absolute_import, division, print_function

import asyncio
import subprocess
import time

from dask_jobqueue import DEBUGCluster
from distributed import Client, as_completed


def sleep_abit(x):
    time.sleep(1)
    return x+10


def test_sync_gather():
    """
    this test is now working
    """
    d = DEBUGCluster(cores=1, memory="1gb", extra=["--no-nanny", "--no-bokeh"])
    d.adapt(minimum=3, maximum=3)
    # d.scale(3)
    c = Client(d)

    while len(c._scheduler_identity["workers"]) < 3:
        continue

    ret = c.map(lambda x: sleep_abit(x), list(range(10)))
    time.sleep(1.2)
    old_pids = []
    for k, v in c._scheduler_identity["workers"].items():
        pid = int(v['id'].split('--')[-2])
        old_pids.append(pid)
        subprocess.call("kill -9 {}".format(pid).split())
    not_done_count = sum([1 for task in ret if task.status == 'pending'])
    assert not_done_count == 7

    final_ret = c.gather(ret)
    new_pids = []
    for k, v in c._scheduler_identity["workers"].items():
        pid = int(v['id'].split('--')[-2])
        new_pids.append(pid)
    for pid in new_pids:
        assert pid not in old_pids

    assert len(final_ret) == 10
    c.close()


async def _a_g_main():
    d = DEBUGCluster(cores=1, memory="1gb", extra=["--no-nanny", "--no-bokeh"])
    d.adapt(minimum=3, maximum=3)
    # d.scale(3)
    c = await Client(d, asynchronous=True)

    while len(c._scheduler_identity["workers"]) < 3:
        await asyncio.sleep(1)

    ret = c.map(lambda x: sleep_abit(x), list(range(10)))

    await asyncio.sleep(1.2)

    old_pids = []
    for k, v in c._scheduler_identity["workers"].items():
        pid = int(v['id'].split('--')[-2])
        old_pids.append(pid)
        subprocess.call("kill -9 {}".format(pid).split())

    not_done_count = sum([1 for task in ret if task.status == 'pending'])
    assert not_done_count == 7

    final_ret = await asyncio.gather(*ret)
    new_pids = []
    for k, v in c._scheduler_identity["workers"].items():
        pid = int(v['id'].split('--')[-2])
        new_pids.append(pid)
    for pid in new_pids:
        assert pid not in old_pids
    assert len(final_ret) == 10
    c.close()


def test_async_gather():
    """
    this test is now working
    """
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_a_g_main())


def test_sync_as_completed():
    d = DEBUGCluster(cores=1, memory="1gb", extra=["--no-nanny", "--no-bokeh"])
    d.adapt(minimum=3, maximum=3)
    # d.scale(3)
    c = Client(d)

    while len(c._scheduler_identity["workers"]) < 3:
        continue

    ret = c.map(lambda x: sleep_abit(x), list(range(10)))

    old_pids = []
    new_pids = []
    count = 0
    work_queue = as_completed(ret)
    for ret in work_queue:
        result = ret.result()
        count += 1
        if count == 3:
            for k, v in c._scheduler_identity["workers"].items():
                pid = int(v['id'].split('--')[-2])
                old_pids.append(pid)
                subprocess.call("kill -9 {}".format(pid).split())
        elif count == 7:
            for k, v in c._scheduler_identity["workers"].items():
                pid = int(v['id'].split('--')[-2])
                new_pids.append(pid)
    for pid in new_pids:
        assert pid not in old_pids
    assert count == 10
    c.close()


async def _a_c_main():
    d = DEBUGCluster(cores=1, memory="1gb", extra=["--no-nanny", "--no-bokeh"])
    d.adapt(minimum=3, maximum=3)
    # d.scale(3)
    c = await Client(d, asynchronous=True)

    while len(c._scheduler_identity["workers"]) < 3:
        await asyncio.sleep(1)

    ret = c.map(lambda x: sleep_abit(x), list(range(10)))

    old_pids = []
    new_pids = []
    count = 0
    work_queue = as_completed(ret)
    async for ret in work_queue:
        result = await ret
        count += 1
        if count == 3:
            for k, v in c._scheduler_identity["workers"].items():
                pid = int(v['id'].split('--')[-2])
                old_pids.append(pid)
                subprocess.call("kill -9 {}".format(pid).split())
        elif count == 7:
            for k, v in c._scheduler_identity["workers"].items():
                pid = int(v['id'].split('--')[-2])
                new_pids.append(pid)
    for pid in new_pids:
        assert pid not in old_pids
    assert count == 10

    c.close()


def test_async_as_completed():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_a_c_main())


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


# if __name__ == "__main__":
#     if sys.argv[1] == "async_a_c":
#         loop = asyncio.get_event_loop()
#         loop.run_until_complete(a_c_main())
#     if sys.argv[1] == "async_g":
#         loop = asyncio.get_event_loop()
#         loop.run_until_complete(a_g_main())
#     # elif sys.argv[1] == "local":
#     #     local_main()
#     else:
#         main()
