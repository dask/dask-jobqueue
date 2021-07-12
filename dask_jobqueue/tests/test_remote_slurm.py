from time import sleep, time
from typing import Dict

import pytest
from distributed import Client

from dask_jobqueue.remote_slurm import RemoteSLURMCluster
from . import QUEUE_WAIT

TEST_SOCKET_API_PATH = "/var/run/slurm/slurmrestd.socket"


@pytest.fixture(scope="session")
def api_credentials() -> Dict[str, str]:
    return {
        "api_socket_path": TEST_SOCKET_API_PATH,
    }


@pytest.mark.env("slurm_socket")
def test_basic(loop, api_credentials):
    with RemoteSLURMCluster(
        **api_credentials,
        cores=1,
        # processes=1,
        memory="2GB",
        loop=loop,
        log_directory="/var/log/slurm",
        local_directory="/tmp",
    ) as cluster:
        with Client(cluster, loop=loop) as client:
            print("Scaling...")
            cluster.scale(1)
            print("Starting scale wait...")
            client.wait_for_workers(1)

            print("Submitting func...")
            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11

            workers = list(client.scheduler_info()["workers"].values())
            w = workers[0]
            assert w["memory_limit"] == 2e9
            assert w["nthreads"] == 1

            print("Scaling down...")
            cluster.scale(0)

            start = time()
            print("Starting wait for scale down...")
            while client.scheduler_info()["workers"]:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT


@pytest.mark.env("slurm_socket")
def test_adaptive(loop, api_credentials):
    with RemoteSLURMCluster(
        **api_credentials,
        cores=1,
        # processes=1,
        memory="2GB",
        loop=loop,
        log_directory="/var/log/slurm",
        local_directory="/tmp",
    ) as cluster:
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
