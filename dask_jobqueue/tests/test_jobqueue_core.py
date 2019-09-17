import os
import shutil
import socket
import sys
import re

import pytest

from dask_jobqueue import (
    JobQueueCluster,
    PBSCluster,
    MoabCluster,
    SLURMCluster,
    SGECluster,
    LSFCluster,
    OARCluster,
)

from dask_jobqueue.sge import SGEJob


def test_errors():
    with pytest.raises(ValueError, match="Job type.*job_cls="):
        JobQueueCluster(cores=4)


def test_command_template():
    with PBSCluster(cores=2, memory="4GB") as cluster:
        assert (
            "%s -m distributed.cli.dask_worker" % (sys.executable)
            in cluster._dummy_job._command_template
        )
        assert " --nthreads 2" in cluster._dummy_job._command_template
        assert " --memory-limit " in cluster._dummy_job._command_template
        assert " --name " in cluster._dummy_job._command_template

    with PBSCluster(
        cores=2,
        memory="4GB",
        death_timeout=60,
        local_directory="/scratch",
        extra=["--preload", "mymodule"],
    ) as cluster:
        assert " --death-timeout 60" in cluster._dummy_job._command_template
        assert " --local-directory /scratch" in cluster._dummy_job._command_template
        assert " --preload mymodule" in cluster._dummy_job._command_template


@pytest.mark.parametrize(
    "Cluster",
    [PBSCluster, MoabCluster, SLURMCluster, SGECluster, LSFCluster, OARCluster],
)
def test_shebang_settings(Cluster):
    default_shebang = "#!/usr/bin/env bash"
    python_shebang = "#!/usr/bin/python"
    with Cluster(cores=2, memory="4GB", shebang=python_shebang) as cluster:
        job_script = cluster.job_script()
        assert job_script.startswith(python_shebang)
        assert "bash" not in job_script
    with Cluster(cores=2, memory="4GB") as cluster:
        job_script = cluster.job_script()
        assert job_script.startswith(default_shebang)


@pytest.mark.parametrize(
    "Cluster", [PBSCluster, MoabCluster, SLURMCluster, SGECluster, LSFCluster]
)
def test_dashboard_link(Cluster):
    with Cluster(cores=1, memory="1GB") as cluster:
        assert re.match(r"http://\d+\.\d+\.\d+.\d+:\d+/status", cluster.dashboard_link)


def test_forward_ip():
    ip = "127.0.0.1"
    with PBSCluster(
        walltime="00:02:00",
        processes=4,
        cores=8,
        memory="28GB",
        name="dask-worker",
        host=ip,
    ) as cluster:
        assert cluster.scheduler.ip == ip

    default_ip = socket.gethostbyname("")
    with PBSCluster(
        walltime="00:02:00", processes=4, cores=8, memory="28GB", name="dask-worker"
    ) as cluster:
        assert cluster.scheduler.ip == default_ip


@pytest.mark.parametrize("Cluster", [])
@pytest.mark.parametrize(
    "qsub_return_string",
    [
        "{job_id}.admin01",
        "Request {job_id}.asdf was sumbitted to queue: standard.",
        "sbatch: Submitted batch job {job_id}",
        "{job_id};cluster",
        "Job <{job_id}> is submitted to default queue <normal>.",
        "{job_id}",
    ],
)
def test_job_id_from_qsub_legacy(Cluster, qsub_return_string):
    original_job_id = "654321"
    qsub_return_string = qsub_return_string.format(job_id=original_job_id)
    with Cluster(cores=1, memory="1GB") as cluster:
        assert original_job_id == cluster._job_id_from_submit_output(qsub_return_string)


@pytest.mark.parametrize("job_cls", [SGEJob])
@pytest.mark.parametrize(
    "qsub_return_string",
    [
        "{job_id}.admin01",
        "Request {job_id}.asdf was sumbitted to queue: standard.",
        "sbatch: Submitted batch job {job_id}",
        "{job_id};cluster",
        "Job <{job_id}> is submitted to default queue <normal>.",
        "{job_id}",
    ],
)
def test_job_id_from_qsub(job_cls, qsub_return_string):
    original_job_id = "654321"
    qsub_return_string = qsub_return_string.format(job_id=original_job_id)
    job = job_cls(cores=1, memory="1GB")
    assert original_job_id == job._job_id_from_submit_output(qsub_return_string)


@pytest.mark.parametrize("Cluster", [])
def test_job_id_error_handling_legacy(Cluster):
    # non-matching regexp
    with Cluster(cores=1, memory="1GB") as cluster:
        with pytest.raises(ValueError, match="Could not parse job id"):
            return_string = "there is no number here"
            cluster._job_id_from_submit_output(return_string)

    # no job_id named group in the regexp
    with Cluster(cores=1, memory="1GB") as cluster:
        with pytest.raises(ValueError, match="You need to use a 'job_id' named group"):
            return_string = "Job <12345> submitted to <normal>."
            cluster.job_id_regexp = r"(\d+)"
            cluster._job_id_from_submit_output(return_string)


@pytest.mark.parametrize("job_cls", [SGEJob])
def test_job_id_error_handling(job_cls):
    # non-matching regexp
    job = job_cls(cores=1, memory="1GB")
    with pytest.raises(ValueError, match="Could not parse job id"):
        return_string = "there is no number here"
        job._job_id_from_submit_output(return_string)

    # no job_id named group in the regexp
    job = job_cls(cores=1, memory="1GB")
    with pytest.raises(ValueError, match="You need to use a 'job_id' named group"):
        return_string = "Job <12345> submitted to <normal>."
        job.job_id_regexp = r"(\d+)"
        job._job_id_from_submit_output(return_string)


def test_log_directory(tmpdir):
    shutil.rmtree(tmpdir.strpath, ignore_errors=True)
    with PBSCluster(cores=1, memory="1GB"):
        assert not os.path.exists(tmpdir.strpath)

    with PBSCluster(cores=1, memory="1GB", log_directory=tmpdir.strpath):
        assert os.path.exists(tmpdir.strpath)


@pytest.mark.skip
def test_jobqueue_cluster_call(tmpdir):
    cluster = PBSCluster(cores=1, memory="1GB")

    path = tmpdir.join("test.py")
    path.write('print("this is the stdout")')

    out = cluster._call([sys.executable, path.strpath])
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
        cluster._call([sys.executable, path_with_error.strpath])


@pytest.mark.parametrize(
    "Cluster",
    [PBSCluster, MoabCluster, SLURMCluster, SGECluster, LSFCluster, OARCluster],
)
def test_cluster_has_cores_and_memory(Cluster):
    with pytest.raises(ValueError, match=r"cores=\d, memory='\d+GB'"):
        Cluster()

    with pytest.raises(ValueError, match=r"cores=\d, memory='1GB'"):
        Cluster(memory="1GB")

    with pytest.raises(ValueError, match=r"cores=4, memory='\d+GB'"):
        Cluster(cores=4)
