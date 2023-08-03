import re
import sys
from time import sleep, time

import pytest
from distributed import Client

import dask
from dask.utils import format_bytes, parse_bytes

from dask_jobqueue import HTCondorCluster
from dask_jobqueue.core import Job

from . import QUEUE_WAIT


def test_header():
    with HTCondorCluster(cores=1, memory="100MB", disk="100MB") as cluster:
        assert cluster._dummy_job.job_header_dict["MY.DaskWorkerCores"] == 1
        assert cluster._dummy_job.job_header_dict["MY.DaskWorkerDisk"] == 100000000
        assert cluster._dummy_job.job_header_dict["MY.DaskWorkerMemory"] == 100000000


def test_job_script():
    with HTCondorCluster(
        cores=4,
        processes=2,
        memory="100MB",
        disk="100MB",
        job_script_prologue=[
            'export LANG="en_US.utf8"',
            'export LC_ALL="en_US.utf8"',
            "cd /some/path/",
            "source venv/bin/activate",
        ],
        job_extra_directives={"+Extra": "True"},
        submit_command_extra=["-verbose"],
        cancel_command_extra=["-forcex"],
    ) as cluster:
        job_script = cluster.job_script()
        assert "batch_name = dummy-name" in job_script
        assert "RequestCpus = MY.DaskWorkerCores" in job_script
        assert "RequestDisk = floor(MY.DaskWorkerDisk / 1024)" in job_script
        assert "RequestMemory = floor(MY.DaskWorkerMemory / 1048576)" in job_script
        assert "MY.DaskWorkerCores = 4" in job_script
        assert "MY.DaskWorkerDisk = 100000000" in job_script
        assert "MY.DaskWorkerMemory = 100000000" in job_script
        assert 'MY.JobId = "$(ClusterId).$(ProcId)"' in job_script
        assert 'export LANG=""en_US.utf8""' in job_script
        assert 'export LC_ALL=""en_US.utf8""' in job_script
        assert "cd /some/path/" in job_script
        assert "source venv/bin/activate" in job_script
        assert "+Extra = True" in job_script
        assert re.search(
            r"condor_submit\s.*-verbose", cluster._dummy_job.submit_command
        )
        assert re.search(r"condor_rm\s.*-forcex", cluster._dummy_job.cancel_command)

        assert (
            "{} -m distributed.cli.dask_worker tcp://".format(sys.executable)
            in job_script
        )
        formatted_bytes = format_bytes(parse_bytes("50MB")).replace(" ", "")
        assert f"--memory-limit {formatted_bytes}" in job_script
        assert "--nthreads 2" in job_script
        assert "--nworkers 2" in job_script


@pytest.mark.env("htcondor")
def test_basic(loop):
    with HTCondorCluster(
        cores=1, memory="500MiB", disk="100MiB", log_directory="logs", loop=loop
    ) as cluster:
        with Client(cluster) as client:
            cluster.scale(2)

            client.wait_for_workers(2, timeout=QUEUE_WAIT)

            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11

            condor_q = Job._call(["condor_q", "-batch"]).strip()

            workers = list(client.scheduler_info()["workers"].values())
            w = workers[0]
            assert w["memory_limit"] == 500 * 1024**2
            assert w["nthreads"] == 1
            assert w["name"] in condor_q

            cluster.scale(0)

            start = time()
            while client.scheduler_info()["workers"]:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT


@pytest.mark.env("htcondor")
def test_extra_args_broken_cancel(loop):
    with HTCondorCluster(
        cores=1,
        memory="500MiB",
        disk="500MiB",
        log_directory="logs",
        loop=loop,
        cancel_command_extra=["-name", "wrong.docker"],
    ) as cluster:
        with Client(cluster) as client:
            cluster.scale(2)

            client.wait_for_workers(2, timeout=QUEUE_WAIT)
            workers = Job._call(["condor_q", "-af", "jobpid"]).strip()
            assert workers, "we got dask workers"

            cluster.scale(0)

            start = time()
            while client.scheduler_info()["workers"]:
                sleep(0.100)

                workers = Job._call(["condor_q", "-af", "jobpid"]).strip()
                assert workers, "killing workers with broken cancel_command didn't fail"

                if time() > start + QUEUE_WAIT // 3:
                    return

            # Remove running job as it prevents other jobs execution in subsequent tests
            Job._call(["condor_rm", "-all"]).strip()


def test_config_name_htcondor_takes_custom_config():
    conf = {
        "cores": 1,
        "memory": "120 MB",
        "disk": "120 MB",
        "job-extra": None,
        "job-extra-directives": [],
        "name": "myname",
        "processes": 1,
        "interface": None,
        "death-timeout": None,
        "extra": None,
        "worker-extra-args": [],
        "env-extra": None,
        "job-script-prologue": [],
        "log-directory": None,
        "shebang": "#!/usr/bin/env condor_submit",
        "local-directory": "/tmp",
        "shared-temp-directory": None,
    }

    with dask.config.set({"jobqueue.htcondor-config-name": conf}):
        with HTCondorCluster(config_name="htcondor-config-name") as cluster:
            assert cluster.job_name == "myname"
