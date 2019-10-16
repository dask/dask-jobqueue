import sys

import dask
import pytest

from dask_jobqueue import MoabCluster, PBSCluster


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
        project="DaskOnPBS",
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

    with Cluster(cores=4, memory="8GB", job_extra=["-j oe"]) as cluster:

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
        assert "--nthreads 2 --nprocs 4 --memory-limit 7.00GB" in job_script

    with Cluster(
        queue="regular",
        project="DaskOnPBS",
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
        assert "--nthreads 2 --nprocs 4 --memory-limit 7.00GB" in job_script


# TODO make it a common test?
def test_config(loop):
    with dask.config.set(
        {"jobqueue.pbs.walltime": "00:02:00", "jobqueue.pbs.local-directory": "/foo"}
    ):
        with PBSCluster(loop=loop, cores=1, memory="2GB") as cluster:
            assert "00:02:00" in cluster.job_script()
            assert "--local-directory /foo" in cluster.job_script()


# TODO make it a common test
def test_config_name_pbs_takes_custom_config():
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

    with dask.config.set({"jobqueue.pbs-config-name": conf}):
        with PBSCluster(config_name="pbs-config-name") as cluster:
            assert cluster.job_name == "myname"


# TODO common test
def test_informative_errors():
    with pytest.raises(ValueError) as info:
        PBSCluster(memory=None, cores=4)
    assert "memory" in str(info.value)

    with pytest.raises(ValueError) as info:
        PBSCluster(memory="1GB", cores=None)
    assert "cores" in str(info.value)


# TODO Hmmm what is this testing?
@pytest.mark.asyncio
async def test_adapt(loop):
    async with PBSCluster(cores=1, memory="1 GB", asynchronous=True) as cluster:
        cluster.adapt()
