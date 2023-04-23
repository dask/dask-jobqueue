import sys
from time import sleep, time

import pytest
from distributed import Client

import dask

from dask_jobqueue import FluxCluster
from dask_jobqueue.flux import timestr2seconds

QUEUE_WAIT = 120


@pytest.mark.env("flux")
def test_timestring():
    # 2 minutes
    assert timestr2seconds("00:02:00") == 120.0

    # 60 seconds
    assert timestr2seconds("00:00:60") == 60.0

    # Two hours
    assert timestr2seconds("02:00:00") == 7200.0

    # One day, 2 hours
    assert timestr2seconds("1-02:00:00") == 93600.0


@pytest.mark.env("flux")
def test_header():
    with FluxCluster(
        walltime="00:02:00", processes=4, cores=8, memory="28GB"
    ) as cluster:
        assert "#flux:" in cluster.job_header
        assert "#flux: --job-name=dask-worker" in cluster.job_header
        assert "#flux: -n 1" in cluster.job_header
        assert "--mem" not in cluster.job_header
        assert "#flux: -t 120.0" in cluster.job_header
        assert "#flux: --cores-per-slot=8" in cluster.job_header

    with FluxCluster(
        queue="regular",
        processes=4,
        cores=8,
        memory="28GB",
        job_cpu=16,
        job_mem="100G",
    ) as cluster:
        assert "--mem=100G" not in cluster.job_header
        assert "#flux: --queue=regular" in cluster.job_header

    with FluxCluster(cores=4, memory="8GB") as cluster:
        assert "#flux:" in cluster.job_header
        assert "#flux: --cores-per-slot=4" in cluster.job_header
        assert "#flux: --job-name" in cluster.job_header
        assert "#flux: -n 1" in cluster.job_header
        assert "#flux: --queue" not in cluster.job_header


@pytest.mark.env("flux")
def test_job_script():
    with FluxCluster(
        walltime="00:02:00", processes=4, cores=8, memory="28GB"
    ) as cluster:
        job_script = cluster.job_script()
        assert "#flux:" in job_script
        assert "#flux: --job-name=dask-worker" in job_script
        assert "#flux: --cores-per-slot=8" in cluster.job_header
        assert "#flux: -n 1" in job_script
        assert "--mem=27G" not in job_script
        assert "#flux: -t 120.0" in job_script
        assert "#flux: --queue" not in job_script
        assert (
            "{} -m distributed.cli.dask_worker tcp://".format(sys.executable)
            in job_script
        )

    with FluxCluster(
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
        assert 'export LANG="en_US.utf8"' in job_script
        assert 'export LANGUAGE="en_US.utf8"' in job_script
        assert 'export LC_ALL="en_US.utf8"' in job_script

        assert (
            "{} -m distributed.cli.dask_worker tcp://".format(sys.executable)
            in job_script
        )


@pytest.mark.env("flux")
def test_basic(loop):
    with FluxCluster(
        walltime="00:02:00",
        cores=2,
        processes=1,
        memory="2GiB",
        loop=loop,
    ) as cluster:
        with Client(cluster) as client:
            cluster.scale(3)

            start = time()
            client.wait_for_workers(3, timeout=QUEUE_WAIT)

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


@pytest.mark.env("flux")
def test_adaptive(loop):
    with FluxCluster(
        walltime="00:02:00",
        cores=2,
        processes=1,
        memory="2GB",
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


@pytest.mark.env("flux")
def test_config_name_flux_takes_custom_config():
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

    with dask.config.set({"jobqueue.flux.config-name": conf}):
        with FluxCluster(config_name="flux.config-name") as cluster:
            assert cluster.job_name == "myname"


@pytest.mark.env("flux")
def test_worker_name_uses_cluster_name(loop):
    # The environment variable setup below is similar to a job array setup
    # where you would use SLURM_ARRAY_JOB_ID to make sure that Dask workers
    # belonging to the same job array have different worker names
    with FluxCluster(
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


@pytest.mark.env("flux")
def test_unsupported_variables():
    import warnings

    # test issuing of warning
    warnings.simplefilter("always")

    job_cls = FluxCluster.job_cls
    with warnings.catch_warnings(record=True) as w:
        # should give a warning
        job_cls(
            cores=1,
            memory="1 GB",
            project=["project is used"],
            account=["account is used"],
        )
        assert len(w) == 2
        assert issubclass(w[0].category, FutureWarning)
        assert "flux does not have support for project" in str(w[0].message)
        assert "flux does not have support for accounts" in str(w[1].message)

    with warnings.catch_warnings(record=True) as w:
        # should not give a warning
        job_cls(
            cores=1,
            memory="1 GB",
        )
        assert len(w) == 0
