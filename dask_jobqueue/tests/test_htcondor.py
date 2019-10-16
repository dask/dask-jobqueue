import sys

import dask

from dask_jobqueue import HTCondorCluster

QUEUE_WAIT = 30  # seconds


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
        env_extra=['export LANG="en_US.utf8"', 'export LC_ALL="en_US.utf8"'],
        job_extra={"+Extra": "True"},
    ) as cluster:
        job_script = cluster.job_script()
        assert "RequestCpus = MY.DaskWorkerCores" in job_script
        assert "RequestDisk = floor(MY.DaskWorkerDisk / 1024)" in job_script
        assert "RequestMemory = floor(MY.DaskWorkerMemory / 1048576)" in job_script
        assert "MY.DaskWorkerCores = 4" in job_script
        assert "MY.DaskWorkerDisk = 100000000" in job_script
        assert "MY.DaskWorkerMemory = 100000000" in job_script
        assert 'MY.JobId = "$(ClusterId).$(ProcId)"' in job_script
        assert "LANG=en_US.utf8" in job_script
        assert "LC_ALL=en_US.utf8" in job_script
        assert "JOB_ID=$F(MY.JobId)" in job_script
        assert "export" not in job_script
        assert "+Extra = True" in job_script

        assert (
            "{} -m distributed.cli.dask_worker tcp://".format(sys.executable)
            in job_script
        )
        assert "--memory-limit 50.00MB" in job_script
        assert "--nthreads 2" in job_script
        assert "--nprocs 2" in job_script


def test_config_name_htcondor_takes_custom_config():
    conf = {
        "cores": 1,
        "memory": "120 MB",
        "disk": "120 MB",
        "job-extra": [],
        "name": "myname",
        "processes": 1,
        "interface": None,
        "death-timeout": None,
        "extra": [],
        "env-extra": [],
        "log-directory": None,
        "shebang": "#!/usr/bin/env condor_submit",
        "local-directory": "/tmp",
    }

    with dask.config.set({"jobqueue.htcondor-config-name": conf}):
        with HTCondorCluster(config_name="htcondor-config-name") as cluster:
            assert cluster.job_name == "myname"
