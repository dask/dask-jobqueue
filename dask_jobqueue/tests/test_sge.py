from __future__ import absolute_import, division, print_function

from time import sleep, time

import pytest
import subprocess

from distributed import Client
from distributed.utils_test import loop  # noqa: F401

from dask_jobqueue import SGECluster
import dask

from . import QUEUE_WAIT


@pytest.mark.env("sge")  # noqa: F811
def test_basic(loop):  # noqa: F811
    with SGECluster(walltime=QUEUE_WAIT * 2, cores=8, processes=4, memory='2GB', loop=loop) as cluster:
        print(cluster.job_script())
        with Client(cluster, loop=loop) as client:

            cluster.scale(2)

            start = time()
            while not(cluster.pending_jobs or cluster.running_jobs):
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11
            assert cluster.running_jobs

            workers = list(client.scheduler_info()['workers'].values())
            w = workers[0]
            assert w['memory_limit'] == 2e9 / 4
            assert w['ncores'] == 2

            cluster.scale(0)

            start = time()
            while cluster.running_jobs:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT


def test_config_name_sge_takes_custom_config():
    conf = {'queue': 'myqueue',
            'project': 'myproject',
            'ncpus': 1,
            'cores': 1,
            'memory': '2 GB',
            'walltime': '00:02',
            'job-extra': [],
            'name': 'myname',
            'processes': 1,
            'interface': None,
            'death-timeout': None,
            'local-directory': '/foo',
            'extra': [],
            'env-extra': [],
            'log-directory': None,
            'shebang': '#!/usr/bin/env bash',
            'job-cpu': None,
            'job-mem': None,
            'resource-spec': None}

    with dask.config.set({'jobqueue.sge-config-name': conf}):
        with SGECluster(config_name='sge-config-name') as cluster:
            assert cluster.name == 'myname'


@pytest.mark.env("sge")  # noqa: F811
def test_taskarrays(loop):  # noqa: F811

    def scale_cluster(cluster, size):
        """ Starts a small cluster and returns information on jobs and tasks """
        cluster.scale(size)
        # waiting to allow cluster to start (no pending jobs)
        sleep(2)
        while cluster.pending_jobs:
            sleep(1)
        sleep(2)
        len_expected_jobs = len(list(cluster.pending_jobs.keys()) + list(cluster.running_jobs.keys()))
        lines = [line for line in subprocess.check_output('qstat').decode().split('\n') if 'dask-work' in line]
        len_unique_jids = len(set(line.split()[0] for line in lines))
        len_scheduler_workers = len(cluster.scheduler.get_ncores())
        return len_expected_jobs, len_unique_jids, len_scheduler_workers

    with SGECluster(walltime=QUEUE_WAIT * 4, cores=1, processes=1, memory='2GB', loop=loop) as cluster:

        # Test starting up one single core
        len_expected_jobs, len_unique_jids, len_scheduler_workers = scale_cluster(cluster, 1)
        assert len_expected_jobs == 1, 'There should be one unique job registered in the dask cluster. Found'.format(len_expected_jobs)
        assert len_scheduler_workers == 1, 'There should be one worker registered in the scheduler. Found'.format(len_scheduler_workers)
        assert len_unique_jids == 1, 'There should be one unique job running on SGE. Found'.format(len_unique_jids)

        # Test adding 5 more jobs in one task array
        len_expected_jobs, len_unique_jids, len_scheduler_workers = scale_cluster(cluster, 6)
        assert len_expected_jobs == 6, 'There should be six unique jobs registered in the dask cluster. Found'.format(len_expected_jobs)
        assert len_scheduler_workers == 6, 'There should be six workers registered in the scheduler. Found'.format(len_scheduler_workers)
        assert len_unique_jids == 2, 'There should be two unique jobs running on SGE. Found'.format(len_unique_jids)        

        # Test closing all task arrays
        len_expected_jobs, len_unique_jids, len_scheduler_workers = scale_cluster(cluster, 0)
        assert len_expected_jobs == 0, 'There should be no more unique jobs registered in the dask cluster. Found'.format(len_expected_jobs)
        assert len_scheduler_workers == 0, 'There should be no more workers registered in the scheduler. Found'.format(len_scheduler_workers)
        assert len_unique_jids == 0, 'There should be no more jobs running on SGE. Found'.format(len_unique_jids)
