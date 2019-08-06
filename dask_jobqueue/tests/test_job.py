from dask_jobqueue import PBSJob, SGEJob
from dask_jobqueue.job import JobQueueCluster
from dask.distributed import Scheduler, Client
import pytest


def test_basic():
    job = PBSJob(scheduler="127.0.0.1:12345", cores=1, memory="1 GB")
    assert "127.0.0.1:12345" in job.job_script()


@pytest.mark.env("pbs")
@pytest.mark.asyncio
async def test_live():
    async with Scheduler(port=0) as s:
        job = PBSJob(scheduler=s.address, name="foo", cores=1, memory="1GB")
        job = await job
        async with Client(s.address, asynchronous=True) as client:
            await client.wait_for_workers(1)
            worker_name = list(s.workers.values())[0].name
            assert worker_name.startswith("foo")
            assert job.job_id in worker_name


@pytest.mark.env("pbs")
@pytest.mark.asyncio
async def test_pbs_cluster():
    async with JobQueueCluster(cores=1, memory="1GB", Job=PBSJob,
            asynchronous=True) as cluster:
        cluster.scale(2)
        await cluster
        assert len(cluster.workers) == 2
        assert all(isinstance(w, PBSJob) for w in cluster.workers.values())
        assert all(w.status == "running" for w in cluster.workers.values())


@pytest.mark.env("sge")
@pytest.mark.asyncio
async def test_live_sge():
    async with Scheduler(port=0) as s:
        job = SGEJob(scheduler=s.address, name="foo", cores=1, memory="1GB")
        job = await job
        async with Client(s.address, asynchronous=True) as client:
            await client.wait_for_workers(1)
            worker_name = list(s.workers.values())[0].name
            assert worker_name.startswith("foo")
            assert job.job_id in worker_name


@pytest.mark.env("sge")
@pytest.mark.asyncio
async def test_sge_cluster():
    async with JobQueueCluster(cores=1, memory="1GB", Job=SGEJob,
            asynchronous=True) as cluster:
        cluster.scale(2)
        await cluster
        assert len(cluster.workers) == 2
        assert all(isinstance(w, SGEJob) for w in cluster.workers.values())
        assert all(w.status == "running" for w in cluster.workers.values())
