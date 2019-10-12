from time import sleep, time

import pytest
from distributed import Client

from dask_jobqueue import SGECluster
import dask

from . import QUEUE_WAIT


def test_config_name_sge_takes_custom_config():
    conf = {
        "queue": "myqueue",
        "project": "myproject",
        "ncpus": 1,
        "cores": 1,
        "memory": "2 GB",
        "walltime": "00:02",
        "job-extra": [],
        "name": "myname",
        "processes": 1,
        "interface": None,
        "death-timeout": None,
        "local-directory": "/foo",
        "extra": [],
        "env-extra": [],
        "log-directory": None,
        "shebang": "#!/usr/bin/env bash",
        "job-cpu": None,
        "job-mem": None,
        "resource-spec": None,
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
        env_extra=["export MY_VAR=my_var"],
        job_extra=["-w e", "-m e"],
        log_directory=log_directory,
        resource_spec="h_vmem=12G,mem_req=12G",
    ) as cluster:
        job_script = cluster.job_script()
        for each in [
            "--nprocs 2",
            "--nthreads 3",
            "--memory-limit 6.00GB",
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
        ]:
            assert each in job_script


# TODO what should I do with this one? Should I do it for all clusters?
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
