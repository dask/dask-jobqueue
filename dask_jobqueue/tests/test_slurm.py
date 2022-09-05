import sys
from time import sleep, time

import pytest
from distributed import Client

import dask
from dask.utils import format_bytes, parse_bytes

from dask_jobqueue import SLURMCluster

from . import QUEUE_WAIT


def test_header():
    with SLURMCluster(
        walltime="00:02:00", processes=4, cores=8, memory="28GB"
    ) as cluster:

        assert "#SBATCH" in cluster.job_header
        assert "#SBATCH -J dask-worker" in cluster.job_header
        assert "#SBATCH -n 1" in cluster.job_header
        assert "#SBATCH --cpus-per-task=8" in cluster.job_header
        assert "#SBATCH --mem=27G" in cluster.job_header
        assert "#SBATCH -t 00:02:00" in cluster.job_header
        assert "#SBATCH -p" not in cluster.job_header
        # assert "#SBATCH -A" not in cluster.job_header

    with SLURMCluster(
        queue="regular",
        account="DaskOnSlurm",
        processes=4,
        cores=8,
        memory="28GB",
        job_cpu=16,
        job_mem="100G",
    ) as cluster:

        assert "#SBATCH --cpus-per-task=16" in cluster.job_header
        assert "#SBATCH --cpus-per-task=8" not in cluster.job_header
        assert "#SBATCH --mem=100G" in cluster.job_header
        assert "#SBATCH -t " in cluster.job_header
        assert "#SBATCH -A DaskOnSlurm" in cluster.job_header
        assert "#SBATCH -p regular" in cluster.job_header

    with SLURMCluster(cores=4, memory="8GB") as cluster:

        assert "#SBATCH" in cluster.job_header
        assert "#SBATCH -J " in cluster.job_header
        assert "#SBATCH -n 1" in cluster.job_header
        assert "#SBATCH -t " in cluster.job_header
        assert "#SBATCH -p" not in cluster.job_header
        # assert "#SBATCH -A" not in cluster.job_header


def test_job_script():
    with SLURMCluster(
        walltime="00:02:00", processes=4, cores=8, memory="28GB"
    ) as cluster:

        job_script = cluster.job_script()
        assert "#SBATCH" in job_script
        assert "#SBATCH -J dask-worker" in job_script
        formatted_bytes = format_bytes(parse_bytes("7GB")).replace(" ", "")
        assert f"--memory-limit {formatted_bytes}" in job_script
        assert "#SBATCH -n 1" in job_script
        assert "#SBATCH --cpus-per-task=8" in job_script
        assert "#SBATCH --mem=27G" in job_script
        assert "#SBATCH -t 00:02:00" in job_script
        assert "#SBATCH -p" not in job_script
        # assert "#SBATCH -A" not in job_script

        assert "export " not in job_script

        assert (
            "{} -m distributed.cli.dask_worker tcp://".format(sys.executable)
            in job_script
        )
        formatted_bytes = format_bytes(parse_bytes("7GB")).replace(" ", "")
        assert (
            f"--nthreads 2 --nworkers 4 --memory-limit {formatted_bytes}" in job_script
        )

    with SLURMCluster(
        walltime="00:02:00",
        processes=4,
        cores=8,
        memory="28GB",
        job_script_prologue=[
            'export LANG="en_US.utf8"',
            'export LANGUAGE="en_US.utf8"',
            'export LC_ALL="en_US.utf8"',
        ],
    ) as cluster:
        job_script = cluster.job_script()
        assert "#SBATCH" in job_script
        assert "#SBATCH -J dask-worker" in job_script
        assert "#SBATCH -n 1" in job_script
        assert "#SBATCH --cpus-per-task=8" in job_script
        assert "#SBATCH --mem=27G" in job_script
        assert "#SBATCH -t 00:02:00" in job_script
        assert "#SBATCH -p" not in job_script
        # assert "#SBATCH -A" not in job_script

        assert 'export LANG="en_US.utf8"' in job_script
        assert 'export LANGUAGE="en_US.utf8"' in job_script
        assert 'export LC_ALL="en_US.utf8"' in job_script

        assert (
            "{} -m distributed.cli.dask_worker tcp://".format(sys.executable)
            in job_script
        )
        formatted_bytes = format_bytes(parse_bytes("7GB")).replace(" ", "")
        assert (
            f"--nthreads 2 --nworkers 4 --memory-limit {formatted_bytes}" in job_script
        )


@pytest.mark.env("slurm")
def test_basic(loop):
    with SLURMCluster(
        walltime="00:02:00",
        cores=2,
        processes=1,
        memory="2GiB",
        # job_extra_directives=["-D /"],
        loop=loop,
    ) as cluster:
        with Client(cluster) as client:

            cluster.scale(2)

            start = time()
            client.wait_for_workers(2, timeout=QUEUE_WAIT)

            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11

            workers = list(client.scheduler_info()["workers"].values())
            w = workers[0]
            assert w["memory_limit"] == 2 * 1024**3
            assert w["nthreads"] == 2

            cluster.scale(0)

            start = time()
            while client.scheduler_info()["workers"]:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT


@pytest.mark.env("slurm")
def test_adaptive(loop):
    with SLURMCluster(
        walltime="00:02:00",
        cores=2,
        processes=1,
        memory="2GB",
        # job_extra_directives=["-D /"],
        loop=loop,
    ) as cluster:
        cluster.adapt()
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)

            start = time()
            client.wait_for_workers(1, timeout=QUEUE_WAIT)

            assert future.result(QUEUE_WAIT) == 11

            del future

            start = time()
            while client.scheduler_info()["workers"]:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT


def test_config_name_slurm_takes_custom_config():
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
        "worker-extra-args": [],
        "env-extra": None,
        "job-script-prologue": [],
        "log-directory": None,
        "shebang": "#!/usr/bin/env bash",
        "job-cpu": None,
        "job-mem": None,
    }

    with dask.config.set({"jobqueue.slurm-config-name": conf}):
        with SLURMCluster(config_name="slurm-config-name") as cluster:
            assert cluster.job_name == "myname"


@pytest.mark.env("slurm")
def test_different_interfaces_on_scheduler_and_workers(loop):
    with SLURMCluster(
        walltime="00:02:00",
        cores=1,
        memory="2GB",
        interface="eth0:worker",
        scheduler_options={"interface": "eth0:scheduler"},
        loop=loop,
    ) as cluster:
        cluster.scale(jobs=1)
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)

            client.wait_for_workers(1, timeout=QUEUE_WAIT)

            assert future.result(QUEUE_WAIT) == 11


@pytest.mark.env("slurm")
def test_worker_name_uses_cluster_name(loop):
    # The environment variable setup below is similar to a job array setup
    # where you would use SLURM_ARRAY_JOB_ID to make sure that Dask workers
    # belonging to the same job array have different worker names
    with SLURMCluster(
        cores=1,
        memory="2GB",
        name="test-$MY_ENV_VARIABLE",
        job_script_prologue=["MY_ENV_VARIABLE=my-env-variable-value"],
        loop=loop,
    ) as cluster:
        with Client(cluster) as client:
            cluster.scale(jobs=2)
            print(cluster.job_script())
            client.wait_for_workers(2, timeout=QUEUE_WAIT)
            worker_names = [
                w["id"] for w in client.scheduler_info()["workers"].values()
            ]
            assert sorted(worker_names) == [
                "test-my-env-variable-value-0",
                "test-my-env-variable-value-1",
            ]


def test_deprecation_project():
    import warnings

    # test issuing of warning
    warnings.simplefilter("always")

    job_cls = SLURMCluster.job_cls
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
