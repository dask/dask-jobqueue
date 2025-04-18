from time import sleep, time

import pytest
from distributed import Client

from dask_jobqueue import SGECluster
import dask
from dask.utils import format_bytes, parse_bytes

from . import QUEUE_WAIT


@pytest.mark.env("sge")
def test_basic(loop):
    with SGECluster(
        walltime="00:02:00", cores=8, processes=4, memory="2GiB", loop=loop
    ) as cluster:
        with Client(cluster, loop=loop) as client:
            cluster.scale(2)

            start = time()
            while not client.scheduler_info()["workers"]:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11
            assert len(client.scheduler_info()["workers"]) > 0

            workers = list(client.scheduler_info()["workers"].values())
            w = workers[0]
            assert w["memory_limit"] == 2 * 1024**3 / 4
            assert w["nthreads"] == 2

            cluster.scale(0)

            start = time()
            while client.scheduler_info()["workers"]:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT


def test_config_name_sge_takes_custom_config():
    conf = {
        "queue": "myqueue",
        "project": "myproject",
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

    with dask.config.set({"jobqueue.sge-config-name": conf}):
        with SGECluster(config_name="sge-config-name") as cluster:
            assert cluster.job_name == "myname"


def test_job_script(tmpdir):
    log_directory = tmpdir.strpath
    with SGECluster(
        cores=6,
        processes=2,
        memory="12GB",
        queue="my-queue",
        project="my-project",
        walltime="02:00:00",
        job_script_prologue=["export MY_VAR=my_var"],
        job_script_epilogue=[
            'echo "Job finished"',
        ],
        job_extra_directives=["-w e", "-m e"],
        log_directory=log_directory,
        resource_spec="h_vmem=12G,mem_req=12G",
    ) as cluster:
        job_script = cluster.job_script()
        formatted_bytes = format_bytes(parse_bytes("6GB")).replace(" ", "")

        for each in [
            "--nworkers 2",
            "--nthreads 3",
            f"--memory-limit {formatted_bytes}",
            "-q my-queue",
            "-P my-project",
            "-l h_rt=02:00:00",
            "export MY_VAR=my_var",
            "#$ -w e",
            "#$ -m e",
            "#$ -e {}".format(log_directory),
            "#$ -o {}".format(log_directory),
            "-l h_vmem=12G,mem_req=12G",
            "#$ -cwd",
            "#$ -j y",
            'echo "Job finished"',
        ]:
            assert each in job_script


@pytest.mark.env("sge")
def test_complex_cancel_command(loop):
    with SGECluster(
        walltime="00:02:00", cores=1, processes=1, memory="2GB", loop=loop
    ) as cluster:
        with Client(cluster) as client:
            username = "root"
            cluster.cancel_command = "qdel -u {}".format(username)

            cluster.scale(2)

            start = time()
            while not client.scheduler_info()["workers"]:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            cluster.scale(0)

            start = time()
            while client.scheduler_info()["workers"]:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT
