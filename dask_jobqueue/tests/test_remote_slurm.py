from time import sleep, time

import pytest
from distributed import Client

from dask_jobqueue.remote_slurm import RemoteSLURMCluster
from . import QUEUE_WAIT

TEST_SOCKET_API_PATH = "/var/run/slurm/slurmrestd.socket"


@pytest.fixture
def socket_slurm_cluster(loop):
    return RemoteSLURMCluster.with_socket_api_client(
        socket_api_path=TEST_SOCKET_API_PATH,
        cores=2,
        processes=1,
        memory="2GB",
        job_mem="2GB",
        loop=loop,
        log_directory="/var/log/slurm",
        local_directory="/tmp",
    )


@pytest.fixture(params=["socket_slurm_cluster"])
def remote_cluster(request):
    return request.getfixturevalue(request.param)


@pytest.mark.env("slurm_remote")
def test_basic(loop, remote_cluster):
    with remote_cluster as cluster:
        with Client(cluster, loop=loop) as client:
            print("Scaling...")
            cluster.scale(2)
            print("Starting scale wait...")
            client.wait_for_workers(2)

            print("Submitting func...")
            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11

            workers = list(client.scheduler_info()["workers"].values())
            w = workers[0]
            assert w["memory_limit"] == 2e9
            assert w["nthreads"] == 2

            print("Scaling down...")
            cluster.scale(0)

            start = time()
            print("Starting wait for scale down...")
            while client.scheduler_info()["workers"]:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT


@pytest.mark.env("slurm_remote")
def test_adaptive(loop, remote_cluster):
    with remote_cluster as cluster:
        cluster.adapt()
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)

            client.wait_for_workers(1)

            assert future.result(QUEUE_WAIT) == 11

            del future

            start = time()
            while client.scheduler_info()["workers"]:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT
