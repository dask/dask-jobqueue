import asyncio
from time import time
import sys
import re


from dask_jobqueue import PBSCluster, SLURMCluster, SGECluster, OARCluster
from dask_jobqueue.local import LocalCluster
from dask_jobqueue.pbs import PBSJob
from dask_jobqueue.core import JobQueueCluster
from dask.distributed import Scheduler, Client
from dask_jobqueue.tests import QUEUE_WAIT
from distributed.core import Status

import pytest


def test_basic(Cluster):
    job_cls = Cluster.job_cls
    job = job_cls(scheduler="127.0.0.1:12345", cores=1, memory="1 GB")
    assert "127.0.0.1:12345" in job.job_script()


@pytest.mark.asyncio
async def test_job(EnvSpecificCluster):
    job_cls = EnvSpecificCluster.job_cls
    async with Scheduler(port=0) as s:
        job = job_cls(scheduler=s.address, name="foo", cores=1, memory="1GB")
        job = await job
        async with Client(s.address, asynchronous=True) as client:
            await client.wait_for_workers(1, timeout=QUEUE_WAIT)
            assert list(s.workers.values())[0].name == "foo"

        await job.close()

        start = time()
        while len(s.workers):
            await asyncio.sleep(0.1)
            assert time() < start + 10


@pytest.mark.asyncio
async def test_cluster(EnvSpecificCluster):
    job_cls = EnvSpecificCluster.job_cls
    async with JobQueueCluster(
        1, cores=1, memory="1GB", job_cls=job_cls, asynchronous=True, name="foo"
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            assert len(cluster.workers) == 1
            cluster.scale(jobs=2)
            await cluster
            assert len(cluster.workers) == 2
            assert all(isinstance(w, job_cls) for w in cluster.workers.values())
            assert all(w.status == Status.running for w in cluster.workers.values())
            await client.wait_for_workers(2, timeout=QUEUE_WAIT)

            cluster.scale(1)
            start = time()
            await cluster
            while len(cluster.scheduler.workers) > 1:
                await asyncio.sleep(0.1)
                assert time() < start + 10


@pytest.mark.asyncio
async def test_adapt(EnvSpecificCluster):
    job_cls = EnvSpecificCluster.job_cls
    async with JobQueueCluster(
        1, cores=1, memory="1GB", job_cls=job_cls, asynchronous=True, name="foo"
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            await client.wait_for_workers(1, timeout=QUEUE_WAIT)
            cluster.adapt(minimum=0, maximum=4, interval="10ms")

            start = time()
            while len(cluster.scheduler.workers) or cluster.workers:
                await asyncio.sleep(0.050)
                assert time() < start + 10
            assert not cluster.worker_spec
            assert not cluster.workers

            future = client.submit(lambda: 0)
            await client.wait_for_workers(1, timeout=QUEUE_WAIT)

            del future

            start = time()
            while len(cluster.scheduler.workers) or cluster.workers:
                await asyncio.sleep(0.050)
                assert time() < start + 10
            assert not cluster.worker_spec
            assert not cluster.workers


@pytest.mark.asyncio
async def test_adapt_parameters(EnvSpecificCluster):
    job_cls = EnvSpecificCluster.job_cls
    async with JobQueueCluster(
        cores=2, memory="1GB", processes=2, job_cls=job_cls, asynchronous=True
    ) as cluster:
        adapt = cluster.adapt(minimum=2, maximum=4, interval="10ms")
        await adapt.adapt()
        await cluster
        assert len(cluster.workers) == 1  # 2 workers, 4 jobs

        adapt = cluster.adapt(minimum_jobs=2, maximum_jobs=4, interval="10ms")
        await adapt.adapt()
        await cluster
        assert len(cluster.workers) == 2  # 2 workers, 4 jobs


def test_header_lines_skip():
    job = PBSJob(cores=1, memory="1GB", job_name="foobar")
    assert "foobar" in job.job_script()

    job = PBSJob(cores=1, memory="1GB", job_name="foobar", job_directives_skip=["-N"])
    assert "foobar" not in job.job_script()


def test_header_lines_dont_skip_extra_directives():
    job = PBSJob(
        cores=1, memory="1GB", job_name="foobar", job_extra_directives=["-N 123"]
    )
    assert "foobar" in job.job_script()
    assert "-N 123" in job.job_script()

    job = PBSJob(
        cores=1,
        memory="1GB",
        job_name="foobar",
        job_directives_skip=["-N"],
        job_extra_directives=["-N 123"],
    )
    assert "foobar" not in job.job_script()
    assert "-N 123" in job.job_script()


# Test only header_skip for the cluster implementation that uses job_name.


@pytest.mark.parametrize("Cluster", [PBSCluster, SLURMCluster, SGECluster, OARCluster])
def test_deprecation_header_skip(Cluster):
    import warnings

    # test issuing of warning but ignore UserWarning
    warnings.simplefilter("ignore", UserWarning)

    job_cls = Cluster.job_cls
    with warnings.catch_warnings(record=True) as w:
        # should give a warning
        job = job_cls(cores=1, memory="1 GB", header_skip=["old_param"])
        assert len(w) == 1
        assert issubclass(w[0].category, FutureWarning)
        assert "header_skip has been renamed" in str(w[0].message)
    with warnings.catch_warnings(record=True) as w:
        # should give a warning
        job = job_cls(
            cores=1,
            memory="1 GB",
            header_skip=["old_param"],
            job_directives_skip=["new_param"],
        )
        assert len(w) == 1
        assert issubclass(w[0].category, FutureWarning)
        assert "header_skip has been renamed" in str(w[0].message)
    with warnings.catch_warnings(record=True) as w:
        # should not give a warning
        job = job_cls(
            cores=1,
            memory="1 GB",
            job_directives_skip=["new_param"],
        )
        assert len(w) == 0

    # the rest is not about the warning but about behaviour: if job_directives_skip is not
    # set, header_skip should still be used if provided
    warnings.simplefilter("ignore")
    job = job_cls(
        cores=1,
        memory="1 GB",
        job_name="jobname",
        header_skip=["jobname"],
        job_directives_skip=["new_param"],
    )
    job_script = job.job_script()
    assert "jobname" in job_script

    job = job_cls(
        cores=1,
        memory="1 GB",
        job_name="jobname",
        header_skip=["jobname"],
    )
    job_script = job.job_script()
    assert "jobname" not in job_script

    job = job_cls(
        cores=1,
        memory="1 GB",
        job_name="jobname",
        header_skip=["jobname"],
        job_directives_skip=(),
    )
    job_script = job.job_script()
    assert "jobname" not in job_script


@pytest.mark.asyncio
async def test_nworkers_scale():
    async with LocalCluster(
        cores=2, memory="4GB", processes=2, asynchronous=True
    ) as cluster:
        s = cluster.scheduler
        async with Client(cluster, asynchronous=True) as client:
            cluster.scale(cores=2)
            await cluster
            await client.wait_for_workers(2, timeout=QUEUE_WAIT)
            assert len(cluster.workers) == 1  # two workers, one job
            assert len(s.workers) == 2
            assert cluster.plan == {ws.name for ws in s.workers.values()}

            cluster.scale(cores=1)
            await cluster
            await asyncio.sleep(0.2)
            assert len(cluster.scheduler.workers) == 2  # they're still one group

            cluster.scale(jobs=2)
            assert len(cluster.worker_spec) == 2
            cluster.scale(5)
            assert len(cluster.worker_spec) == 3
            cluster.scale(1)
            assert len(cluster.worker_spec) == 1


def test_docstring_cluster(Cluster):
    assert "cores :" in Cluster.__doc__
    assert Cluster.__name__[: -len("Cluster")] in Cluster.__doc__


def test_deprecation_env_extra(Cluster):
    import warnings

    # test issuing of warning but ignore UserWarning
    warnings.simplefilter("ignore", UserWarning)

    job_cls = Cluster.job_cls
    with warnings.catch_warnings(record=True) as w:
        # should give a warning
        job = job_cls(cores=1, memory="1 GB", env_extra=["env_extra is used"])
        assert len(w) == 1
        assert issubclass(w[0].category, FutureWarning)
        assert "env_extra has been renamed" in str(w[0].message)
    with warnings.catch_warnings(record=True) as w:
        # should give a warning
        job = job_cls(
            cores=1,
            memory="1 GB",
            env_extra=["env_extra is used"],
            job_script_prologue=["job_script_prologue is used"],
        )
        assert len(w) == 1
        assert issubclass(w[0].category, FutureWarning)
        assert "env_extra has been renamed" in str(w[0].message)
    with warnings.catch_warnings(record=True) as w:
        # should not give a warning
        job = job_cls(
            cores=1,
            memory="1 GB",
            job_script_prologue=["job_script_prologue is used, env_extra not"],
        )
        assert len(w) == 0

    # the rest is not about the warning but about behaviour: if job_script_prologue is not
    # set, env_extra should still be used if provided
    warnings.simplefilter("ignore")
    job = job_cls(
        cores=1,
        memory="1 GB",
        env_extra=["env_extra"],
        job_script_prologue=["job_script_prologue"],
    )
    job_script = job.job_script()
    assert "env_extra" not in job_script
    assert "job_script_prologue" in job_script

    job = job_cls(
        cores=1,
        memory="1 GB",
        env_extra=["env_extra"],
    )
    job_script = job.job_script()
    assert "env_extra" in job_script

    job = job_cls(
        cores=1,
        memory="1 GB",
        env_extra=["env_extra"],
        job_script_prologue=[],
    )
    job_script = job.job_script()
    assert "env_extra" in job_script


def test_deprecation_extra(Cluster):
    import warnings

    # test issuing of warning but ignore UserWarning
    warnings.simplefilter("ignore", UserWarning)

    job_cls = Cluster.job_cls
    with warnings.catch_warnings(record=True) as w:
        # should give a warning
        job = job_cls(cores=1, memory="1 GB", extra=["old_param"])
        assert len(w) == 1
        assert issubclass(w[0].category, FutureWarning)
        assert "extra has been renamed" in str(w[0].message)
    with warnings.catch_warnings(record=True) as w:
        # should give a warning
        job = job_cls(
            cores=1,
            memory="1 GB",
            extra=["old_param"],
            worker_extra_args=["new_param"],
        )
        assert len(w) == 1
        assert issubclass(w[0].category, FutureWarning)
        assert "extra has been renamed" in str(w[0].message)
    with warnings.catch_warnings(record=True) as w:
        # should not give a warning
        job = job_cls(
            cores=1,
            memory="1 GB",
            worker_extra_args=["new_param"],
        )
        assert len(w) == 0

    # the rest is not about the warning but about behaviour: if worker_extra_args is not
    # set, extra should still be used if provided
    warnings.simplefilter("ignore")
    job = job_cls(
        cores=1,
        memory="1 GB",
        extra=["old_param"],
        worker_extra_args=["new_param"],
    )
    job_script = job.job_script()
    assert "old_param" not in job_script
    assert "new_param" in job_script

    job = job_cls(
        cores=1,
        memory="1 GB",
        extra=["old_param"],
    )
    job_script = job.job_script()
    assert "old_param" in job_script

    job = job_cls(
        cores=1,
        memory="1 GB",
        extra=["old_param"],
        worker_extra_args=[],
    )
    job_script = job.job_script()
    assert "old_param" in job_script


def test_deprecation_job_extra(Cluster):
    if issubclass(Cluster, LocalCluster):
        return  # nothing to test in this implentation, really

    import warnings

    # test issuing of warning but ignore UserWarning
    warnings.simplefilter("ignore", UserWarning)

    job_cls = Cluster.job_cls
    with warnings.catch_warnings(record=True) as w:
        # should give a warning
        job = job_cls(cores=1, memory="1 GB", job_extra={"old_param": "1"})
        assert len(w) == 1
        assert issubclass(w[0].category, FutureWarning)
        assert "job_extra has been renamed" in str(w[0].message)
    with warnings.catch_warnings(record=True) as w:
        # should give a warning
        job = job_cls(
            cores=1,
            memory="1 GB",
            job_extra={"old_param": "1"},
            job_extra_directives={"new_param": "2"},
        )
        assert len(w) == 1
        assert issubclass(w[0].category, FutureWarning)
        assert "job_extra has been renamed" in str(w[0].message)
    with warnings.catch_warnings(record=True) as w:
        # should not give a warning
        job = job_cls(
            cores=1,
            memory="1 GB",
            job_extra_directives={"new_param": "2"},
        )
        assert len(w) == 0

    # the rest is not about the warning but about behaviour: if job_extra_directives is not
    # set, job_extra should still be used if provided
    warnings.simplefilter("ignore")
    job = job_cls(
        cores=1,
        memory="1 GB",
        job_extra={"old_param": "1"},
        job_extra_directives={"new_param": "2"},
    )
    job_script = job.job_script()
    assert "old_param" not in job_script
    assert "new_param" in job_script

    job = job_cls(
        cores=1,
        memory="1 GB",
        job_extra={"old_param": "1"},
    )
    job_script = job.job_script()
    assert "old_param" in job_script

    job = job_cls(
        cores=1,
        memory="1 GB",
        job_extra={"old_param": "1"},
        job_extra_directives=[],
    )
    job_script = job.job_script()
    assert "old_param" in job_script


@pytest.mark.asyncio
async def test_jobqueue_job_call(tmpdir, Cluster):
    async with Cluster(cores=1, memory="1GB", asynchronous=True) as cluster:
        path = tmpdir.join("test.py")
        path.write('print("this is the stdout")')

        out = await cluster.job_cls._call([sys.executable, path.strpath])
        assert out == "this is the stdout\n"

        path_with_error = tmpdir.join("non-zero-exit-code.py")
        path_with_error.write('print("this is the stdout")\n1/0')

        match = (
            "Command exited with non-zero exit code.+"
            "Exit code: 1.+"
            "stdout:\nthis is the stdout.+"
            "stderr:.+ZeroDivisionError"
        )

        match = re.compile(match, re.DOTALL)
        with pytest.raises(RuntimeError, match=match):
            await cluster.job_cls._call([sys.executable, path_with_error.strpath])
