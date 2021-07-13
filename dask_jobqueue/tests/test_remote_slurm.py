import subprocess
import sys
from time import sleep, time
from typing import Dict

import pytest
from distributed import Client

from dask_jobqueue.remote_slurm import RemoteSLURMCluster
from . import QUEUE_WAIT

TEST_API_URL = "http://slurmrestd:6818/"
TEST_SOCKET_API_PATH = "/var/run/slurm/slurmrestd.socket"


@pytest.fixture(scope="session")
def api_username() -> str:
    return "slurm"


@pytest.fixture(scope="session")
def api_token(api_username: str) -> str:
    """
    Create and register a JWT token for the `api_username` user according to
    https://slurm.schedmd.com/jwt.html.

    In the slurm setup in ci/slurm this is run in the `slurmrestd` docker container, all
    slurm tests run there.
    """
    out = subprocess.check_output(
        ["scontrol", "token", f"username={api_username}", "lifespan=3600"],
    ).decode(sys.stdout.encoding)
    return out.lstrip("SLURM_JWT=").strip()


@pytest.fixture(scope="session")
def socket_slurm_cluster_api_kwargs() -> Dict[str, str]:
    return {
        "api_socket_path": TEST_SOCKET_API_PATH,
    }


@pytest.fixture
def http_slurm_cluster(loop, api_username, api_token):
    return RemoteSLURMCluster.with_http_api_client(
        http_api_url=TEST_API_URL,
        http_api_user_name=api_username,
        http_api_user_token=api_token,
        cores=2,
        processes=1,
        memory="2GB",
        job_mem="2GB",
        loop=loop,
        log_directory="/var/log/slurm",
        local_directory="/tmp",
    )


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


@pytest.fixture(params=["socket_slurm_cluster", "http_slurm_cluster"])
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
