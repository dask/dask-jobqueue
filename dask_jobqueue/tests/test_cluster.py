import pytest
import asyncio
from time import time

from dask.distributed import Client

import dask_jobqueue
from dask_jobqueue.tests import QUEUE_WAIT
from dask_jobqueue.local import LocalCluster

cluster_classes = [
    getattr(dask_jobqueue, attr)
    for attr in dir(dask_jobqueue)
    if attr.endswith("Cluster")
]
cluster_names = [cls.__name__.replace("Cluster", "").lower() for cls in cluster_classes]

cluster_params = [
    pytest.param(cluster_cls, marks=[pytest.mark.env(cluster_name)])
    for cluster_cls, cluster_name in zip(cluster_classes, cluster_names)
]
cluster_params.append(pytest.param(LocalCluster))


def all_checks():
    checks = [obj for name, obj in globals().items() if name.startswith("check_")]
    print("checks:", checks)
    return checks


async def check_basic(cluster, client):
    cluster.scale(2)

    start = time()
    while not client.scheduler_info()["workers"]:
        await asyncio.sleep(0.100)
        assert time() < start + QUEUE_WAIT

    future = client.submit(lambda x: x + 1, 10)
    assert future.result(QUEUE_WAIT) == 11
    assert len(client.scheduler_info()["workers"]) > 0

    workers = list(client.scheduler_info()["workers"].values())
    w = workers[0]
    assert w["memory_limit"] == 2e9 / 4
    assert w["nthreads"] == 2

    cluster.scale(0)

    start = time()
    while client.scheduler_info()["workers"]:
        asyncio.sleep(0.100)
        assert time() < start + QUEUE_WAIT

    assert not cluster.workers and not cluster.worker_spec


async def check_scale_cores_memory(cluster, client):
    cluster.scale(cores=2)
    client.wait_for_workers(1)

    future = client.submit(lambda x: x + 1, 10)
    assert future.result(QUEUE_WAIT) == 11
    assert cluster.workers

    workers = list(client.scheduler_info()["workers"].values())
    w = workers[0]
    assert w["memory_limit"] == 2e9
    assert w["nthreads"] == 2

    cluster.scale(memory="0GB")

    start = time()
    while client.scheduler_info()["workers"]:
        asyncio.sleep(0.100)
        assert time() < start + QUEUE_WAIT

    assert not cluster.workers


async def check_basic_scale_edge_cases(cluster, client):
    cluster.scale(2)
    cluster.scale(0)

    # Wait to see what happens
    asyncio.sleep(0.2)
    start = time()
    while cluster.workers:
        asyncio.sleep(0.1)
        assert time() < start + QUEUE_WAIT

    assert not cluster.workers


async def check_scale_grouped(cluster, client):
    cluster.scale(4)  # Start 2 jobs

    start = time()

    while len(list(client.scheduler_info()["workers"].values())) != 4:
        asyncio.sleep(0.100)
        assert time() < start + QUEUE_WAIT

    future = client.submit(lambda x: x + 1, 10)
    assert future.result(QUEUE_WAIT) == 11
    # assert cluster.running_jobs

    workers = list(client.scheduler_info()["workers"].values())
    w = workers[0]
    assert w["memory_limit"] == 1e9
    assert w["nthreads"] == 1
    assert len(workers) == 4

    cluster.scale(1)  # Should leave 2 workers, 1 job

    start = time()
    while len(client.scheduler_info()["workers"]) != 2:
        asyncio.sleep(0.100)
        assert time() < start + QUEUE_WAIT

    cluster.scale(0)

    start = time()

    assert not cluster.worker_spec
    while len(client.scheduler_info()["workers"]) != 0:
        asyncio.sleep(0.100)
        assert time() < start + QUEUE_WAIT


async def check_adaptive(cluster, client):
    cluster.adapt()
    with Client(cluster) as client:
        future = client.submit(lambda x: x + 1, 10)

        start = time()
        client.wait_for_workers(1)

        assert future.result(QUEUE_WAIT) == 11

        del future

        start = time()
        while client.scheduler_info()["workers"]:
            asyncio.sleep(0.100)
            assert time() < start + QUEUE_WAIT


async def check_adaptive_grouped(cluster, client):
    cluster.adapt(minimum=1)  # at least 1 worker
    client.wait_for_workers(1)

    future = client.submit(lambda x: x + 1, 10)
    assert future.result(QUEUE_WAIT) == 11

    start = time()
    processes = cluster._dummy_job.worker_processes
    while len(client.scheduler_info()["workers"]) != processes:
        asyncio.sleep(0.1)
        assert time() < start + QUEUE_WAIT


async def check_adaptive_cores_mem(cluster, client):
    cluster.adapt(minimum_cores=0, maximum_memory="4GB")
    future = client.submit(lambda x: x + 1, 10)
    assert future.result(QUEUE_WAIT) == 11

    start = time()
    processes = cluster._dummy_job.worker_processes
    while len(client.scheduler_info()["workers"]) != processes:
        asyncio.sleep(0.1)
        assert time() < start + QUEUE_WAIT

    del future

    start = time()
    while cluster.workers:
        asyncio.sleep(0.100)
        assert time() < start + QUEUE_WAIT


@pytest.mark.parametrize("cluster_cls", cluster_params)
@pytest.mark.parametrize("check", all_checks())
@pytest.mark.asyncio
async def test(cluster_cls, check):
    async with cluster_cls(
        cores=6, memory="6GB", processes=2, asynchronous=True
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            check(cluster, client)
