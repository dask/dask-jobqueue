import asyncio
from time import time

from dask_jobqueue import PBSJob, SGEJob, SLURMJob
from dask_jobqueue.job import JobQueueCluster
from dask.distributed import Scheduler, Client

import pytest


def test_basic():
    job = PBSJob(scheduler="127.0.0.1:12345", cores=1, memory="1 GB")
    assert "127.0.0.1:12345" in job.job_script()


job_params = [
    pytest.param(SGEJob, marks=[pytest.mark.env("sge")]),
    pytest.param(PBSJob, marks=[pytest.mark.env("pbs")]),
    pytest.param(SLURMJob, marks=[pytest.mark.env("slurm")]),
]


@pytest.mark.parametrize("Job", job_params)
@pytest.mark.asyncio
async def test_job(Job):
    async with Scheduler(port=0) as s:
        job = Job(scheduler=s.address, name="foo", cores=1, memory="1GB")
        job = await job
        async with Client(s.address, asynchronous=True) as client:
            await client.wait_for_workers(1)
            assert list(s.workers.values())[0].name == "foo"


@pytest.mark.parametrize("Job", job_params)
@pytest.mark.asyncio
async def test_cluster(Job):
    async with JobQueueCluster(
        1, cores=1, memory="1GB", Job=Job, asynchronous=True, name="foo"
    ) as cluster:
        assert len(cluster.workers) == 1
        cluster.scale(2)
        await cluster
        assert len(cluster.workers) == 2
        assert all(isinstance(w, Job) for w in cluster.workers.values())
        assert all(w.status == "running" for w in cluster.workers.values())

        cluster.scale(1)
        start = time()
        await cluster
        while len(cluster.scheduler.workers) != 1:
            await asyncio.sleep(0.1)
            assert time() < start + 5
