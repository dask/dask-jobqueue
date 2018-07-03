from time import sleep, time

import pytest
from distributed import Client
from distributed.utils_test import loop  # noqa: F401

from dask_jobqueue import SGECluster

from . import QUEUE_WAIT


@pytest.mark.env("sge")  # noqa: F811
def test_basic(loop):  # noqa: F811
    with SGECluster(walltime='00:02:00', cores=8, processes=4, memory='28GB',
                    loop=loop) as cluster:
        with Client(cluster, loop=loop) as client:
            cluster.start_workers(2)
            assert cluster.pending_jobs or cluster.running_jobs
            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11
            assert cluster.running_jobs

            info = client.scheduler_info()
            for w in info['workers'].values():
                assert w['memory_limit'] == 7e9
                assert w['ncores'] == 2

            cluster.stop_workers(info['workers'].values())

            start = time()
            while len(client.scheduler_info()['workers']) > 0:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            assert not cluster.running_jobs
            assert cluster.finished_jobs
