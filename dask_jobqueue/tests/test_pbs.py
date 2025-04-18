import sys
from time import sleep, time

import pytest

import dask
from dask.utils import format_bytes, parse_bytes

from dask.distributed import Client

from dask_jobqueue import MoabCluster, PBSCluster

from . import QUEUE_WAIT


@pytest.mark.parametrize("Cluster", [PBSCluster, MoabCluster])
def test_header(Cluster):
    with Cluster(
        walltime="00:02:00", processes=4, cores=8, memory="28GB", name="dask-worker"
    ) as cluster:
        assert "#PBS" in cluster.job_header
        assert "#PBS -N dask-worker" in cluster.job_header
        assert "#PBS -l select=1:ncpus=8:mem=27GB" in cluster.job_header
        assert "#PBS -l walltime=00:02:00" in cluster.job_header
        assert "#PBS -q" not in cluster.job_header
        assert "#PBS -A" not in cluster.job_header

    with Cluster(
        queue="regular",
        account="DaskOnPBS",
        processes=4,
        cores=8,
        resource_spec="select=1:ncpus=24:mem=100GB",
        memory="28GB",
    ) as cluster:
        assert "#PBS -q regular" in cluster.job_header
        assert "#PBS -N dask-worker" in cluster.job_header
        assert "#PBS -l select=1:ncpus=24:mem=100GB" in cluster.job_header
        assert "#PBS -l select=1:ncpus=8:mem=27GB" not in cluster.job_header
        assert "#PBS -l walltime=" in cluster.job_header
        assert "#PBS -A DaskOnPBS" in cluster.job_header

    with Cluster(cores=4, memory="8GB") as cluster:
        assert "#PBS -j oe" not in cluster.job_header
        assert "#PBS -N" in cluster.job_header
        assert "#PBS -l walltime=" in cluster.job_header
        assert "#PBS -A" not in cluster.job_header
        assert "#PBS -q" not in cluster.job_header

    with Cluster(cores=4, memory="8GB", job_extra_directives=["-j oe"]) as cluster:
        assert "#PBS -j oe" in cluster.job_header
        assert "#PBS -N" in cluster.job_header
        assert "#PBS -l walltime=" in cluster.job_header
        assert "#PBS -A" not in cluster.job_header
        assert "#PBS -q" not in cluster.job_header


@pytest.mark.parametrize("Cluster", [PBSCluster, MoabCluster])
def test_job_script(Cluster):
    with Cluster(walltime="00:02:00", processes=4, cores=8, memory="28GB") as cluster:
        job_script = cluster.job_script()
        assert "#PBS" in job_script
        assert "#PBS -N dask-worker" in job_script
        assert "#PBS -l select=1:ncpus=8:mem=27GB" in job_script
        assert "#PBS -l walltime=00:02:00" in job_script
        assert "#PBS -q" not in job_script
        assert "#PBS -A" not in job_script

        assert (
            "{} -m distributed.cli.dask_worker tcp://".format(sys.executable)
            in job_script
        )
        formatted_bytes = format_bytes(parse_bytes("7GB")).replace(" ", "")
        assert "--nthreads 2" in job_script
        assert "--nworkers 4" in job_script
        assert f"--memory-limit {formatted_bytes}" in job_script

    with Cluster(
        queue="regular",
        account="DaskOnPBS",
        processes=4,
        cores=8,
        resource_spec="select=1:ncpus=24:mem=100GB",
        memory="28GB",
    ) as cluster:
        job_script = cluster.job_script()
        assert "#PBS -q regular" in job_script
        assert "#PBS -N dask-worker" in job_script
        assert "#PBS -l select=1:ncpus=24:mem=100GB" in job_script
        assert "#PBS -l select=1:ncpus=8:mem=27GB" not in job_script
        assert "#PBS -l walltime=" in job_script
        assert "#PBS -A DaskOnPBS" in job_script

        assert (
            "{} -m distributed.cli.dask_worker tcp://".format(sys.executable)
            in job_script
        )
        formatted_bytes = format_bytes(parse_bytes("7GB")).replace(" ", "")
        assert "--nthreads 2" in job_script
        assert "--nworkers 4" in job_script
        assert f"--memory-limit {formatted_bytes}" in job_script


@pytest.mark.env("pbs")
def test_basic(loop):
    with PBSCluster(
        walltime="00:02:00",
        processes=1,
        cores=2,
        memory="2GiB",
        local_directory="/tmp",
        job_extra_directives=["-V"],
        loop=loop,
    ) as cluster:
        with Client(cluster) as client:
            cluster.scale(2)
            client.wait_for_workers(2, timeout=QUEUE_WAIT)

            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11
            # assert cluster.running_jobs

            workers = list(client.scheduler_info()["workers"].values())
            w = workers[0]
            assert w["memory_limit"] == 2 * 1024**3
            assert w["nthreads"] == 2

            cluster.scale(0)

            start = time()
            while client.scheduler_info()["workers"]:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            assert not cluster.workers and not cluster.worker_spec


@pytest.mark.env("pbs")
def test_scale_cores_memory(loop):
    with PBSCluster(
        walltime="00:02:00",
        processes=1,
        cores=2,
        memory="2GiB",
        local_directory="/tmp",
        job_extra_directives=["-V"],
        loop=loop,
    ) as cluster:
        with Client(cluster) as client:
            cluster.scale(cores=2)
            client.wait_for_workers(1, timeout=QUEUE_WAIT)

            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11
            assert cluster.workers

            workers = list(client.scheduler_info()["workers"].values())
            w = workers[0]
            assert w["memory_limit"] == 2 * 1024**3
            assert w["nthreads"] == 2

            cluster.scale(memory="0GB")

            start = time()
            while client.scheduler_info()["workers"]:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            assert not cluster.workers


@pytest.mark.env("pbs")
def test_basic_scale_edge_cases(loop):
    with PBSCluster(
        walltime="00:02:00",
        processes=1,
        cores=2,
        memory="2GB",
        local_directory="/tmp",
        job_extra_directives=["-V"],
        loop=loop,
    ) as cluster:
        cluster.scale(2)
        cluster.scale(0)

        # Wait to see what happens
        sleep(0.2)
        start = time()
        while cluster.workers:
            sleep(0.1)
            assert time() < start + QUEUE_WAIT

        assert not cluster.workers


@pytest.mark.env("pbs")
def test_adaptive(loop):
    with PBSCluster(
        walltime="00:02:00",
        processes=1,
        cores=2,
        memory="2GB",
        local_directory="/tmp",
        job_extra_directives=["-V"],
        loop=loop,
    ) as cluster:
        cluster.adapt()
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11

            start = time()
            processes = cluster._dummy_job.worker_processes
            while len(client.scheduler_info()["workers"]) != processes:
                sleep(0.1)
                assert time() < start + QUEUE_WAIT

            del future

            start = time()
            while client.scheduler_info()["workers"] or cluster.workers:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT


@pytest.mark.env("pbs")
def test_adaptive_grouped(loop):
    with PBSCluster(
        walltime="00:02:00",
        processes=1,
        cores=2,
        memory="2GB",
        local_directory="/tmp",
        job_extra_directives=["-V"],
        loop=loop,
    ) as cluster:
        cluster.adapt(minimum=1)  # at least 1 worker
        with Client(cluster) as client:
            client.wait_for_workers(1, timeout=QUEUE_WAIT)

            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11

            start = time()
            processes = cluster._dummy_job.worker_processes
            while len(client.scheduler_info()["workers"]) != processes:
                sleep(0.1)
                assert time() < start + QUEUE_WAIT


@pytest.mark.env("pbs")
def test_adaptive_cores_mem(loop):
    with PBSCluster(
        walltime="00:02:00",
        processes=1,
        cores=2,
        memory="2GB",
        local_directory="/tmp",
        job_extra_directives=["-V"],
        loop=loop,
    ) as cluster:
        cluster.adapt(minimum_cores=0, maximum_memory="4GB")
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11

            start = time()
            processes = cluster._dummy_job.worker_processes
            while len(client.scheduler_info()["workers"]) != processes:
                sleep(0.1)
                assert time() < start + QUEUE_WAIT

            del future

            start = time()
            while cluster.workers:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT


@pytest.mark.env("pbs")
def test_scale_grouped(loop):
    with PBSCluster(
        walltime="00:02:00",
        processes=2,
        cores=2,
        memory="2GiB",
        local_directory="/tmp",
        job_extra_directives=["-V"],
        loop=loop,
    ) as cluster:
        with Client(cluster) as client:
            cluster.scale(4)  # Start 2 jobs

            start = time()

            while len(list(client.scheduler_info()["workers"].values())) != 4:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11
            # assert cluster.running_jobs

            workers = list(client.scheduler_info()["workers"].values())
            w = workers[0]
            assert w["memory_limit"] == 1024**3
            assert w["nthreads"] == 1
            assert len(workers) == 4

            cluster.scale(1)  # Should leave 2 workers, 1 job

            start = time()
            while len(client.scheduler_info()["workers"]) != 2:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            cluster.scale(0)

            start = time()

            assert not cluster.worker_spec
            while len(client.scheduler_info()["workers"]) != 0:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT


def test_config(loop):
    with dask.config.set(
        {"jobqueue.pbs.walltime": "00:02:00", "jobqueue.pbs.local-directory": "/foo"}
    ):
        with PBSCluster(loop=loop, cores=1, memory="2GB") as cluster:
            assert "00:02:00" in cluster.job_script()
            assert "--local-directory /foo" in cluster.job_script()


def test_config_name_pbs_takes_custom_config():
    conf = {
        "queue": "myqueue",
        "account": "myaccount",
        "ncpus": 1,
        "cores": 1,
        "memory": "2 GB",
        "walltime": "00:02",
        "job-extra": None,
        "job-extra-directives": [],
        "name": "myname",
        "processes": 1,
        "interface": None,
        "death-timeout": None,
        "local-directory": "/foo",
        "shared-temp-directory": None,
        "extra": None,
        "worker-command": None,
        "worker-extra-args": [],
        "env-extra": None,
        "job-script-prologue": [],
        "job-script-epilogue": [],
        "log-directory": None,
        "shebang": "#!/usr/bin/env bash",
        "job-cpu": None,
        "job-mem": None,
        "resource-spec": None,
        "python": None,
    }

    with dask.config.set({"jobqueue.pbs-config-name": conf}):
        with PBSCluster(config_name="pbs-config-name") as cluster:
            assert cluster.job_name == "myname"


def test_informative_errors():
    with pytest.raises(ValueError) as info:
        PBSCluster(memory=None, cores=4)
    assert "memory" in str(info.value)

    with pytest.raises(ValueError) as info:
        PBSCluster(memory="1GB", cores=None)
    assert "cores" in str(info.value)


@pytest.mark.asyncio
async def test_adapt(loop):
    async with PBSCluster(cores=1, memory="1 GB", asynchronous=True) as cluster:
        cluster.adapt()


def test_deprecation_project():
    import warnings

    # test issuing of warning
    warnings.simplefilter("always")

    job_cls = PBSCluster.job_cls
    with warnings.catch_warnings(record=True) as w:
        # should give a warning
        job = job_cls(cores=1, memory="1 GB", project=["project is used"])
        assert len(w) == 1
        assert issubclass(w[0].category, FutureWarning)
        assert "project has been renamed" in str(w[0].message)
    with warnings.catch_warnings(record=True) as w:
        # should give a warning
        job = job_cls(
            cores=1,
            memory="1 GB",
            project=["project is used"],
            account=["account is used"],
        )
        assert len(w) == 1
        assert issubclass(w[0].category, FutureWarning)
        assert "project has been renamed" in str(w[0].message)
    with warnings.catch_warnings(record=True) as w:
        # should not give a warning
        job = job_cls(
            cores=1,
            memory="1 GB",
            account=["account is used, project not"],
        )
        assert len(w) == 0

    # the rest is not about the warning but about behaviour: if account is not
    # set, project should still be used if provided
    warnings.simplefilter("ignore")
    job = job_cls(
        cores=1,
        memory="1 GB",
        project=["project"],
        account=["account"],
    )
    job_script = job.job_script()
    assert "project" not in job_script
    assert "account" in job_script

    job = job_cls(
        cores=1,
        memory="1 GB",
        project=["project"],
    )
    job_script = job.job_script()
    assert "project" in job_script

    job = job_cls(
        cores=1,
        memory="1 GB",
        project=["project"],
        account=[],
    )
    job_script = job.job_script()
    assert "project" in job_script
