from dask_jobqueue import PBSJob
from dask.distributed import Scheduler, Client
from distributed.utils_test import cleanup
import pytest


def test_basic():
    job = PBSJob(scheduler="127.0.0.1:12345", cores=1, memory="1 GB")
    assert "127.0.0.1:12345" in job.job_script()


@pytest.mark.env("pbs")
@pytest.mark.asyncio
async def test_live():
    async with Scheduler(port=0) as s:
        async with PBSJob(s.address, name="foo") as job:
            async with Client(s.address, asynchronous=True) as client:
                await client.wait_for_workers(1)
                assert list(s.workers.values())[0].name == "foo"
