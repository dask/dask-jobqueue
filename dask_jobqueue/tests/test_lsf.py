import sys
from time import sleep, time

import dask
from dask.distributed import Client
from distributed.utils_test import loop  # noqa: F401
import pytest

from dask_jobqueue import LSFCluster


def test_header():
    with LSFCluster(walltime='00:02', processes=4, cores=8, memory='8GB') as cluster:

        assert '#BSUB' in cluster.job_header
        assert '#BSUB -J dask-worker' in cluster.job_header
        assert '#BSUB -n 8' in cluster.job_header
        assert '#BSUB -M 7630' in cluster.job_header
        assert '#BSUB -W 00:02' in cluster.job_header
        assert '#BSUB -q' not in cluster.job_header
        assert '#BSUB -P' not in cluster.job_header

    with LSFCluster(queue='general', project='DaskOnLSF', processes=4, cores=8,
                    memory='28GB', ncpus=24, mem=100000000000) as cluster:

        assert '#BSUB -q general' in cluster.job_header
        assert '#BSUB -J dask-worker' in cluster.job_header
        assert '#BSUB -n 24' in cluster.job_header
        assert '#BSUB -n 8' not in cluster.job_header
        assert '#BSUB -M 95368' in cluster.job_header
        assert '#BSUB -M 26703' not in cluster.job_header
        assert '#BSUB -W' in cluster.job_header
        assert '#BSUB -P DaskOnLSF' in cluster.job_header

    with LSFCluster(cores=4, memory='8GB') as cluster:

        assert '#BSUB -n' in cluster.job_header
        assert '#BSUB -W' in cluster.job_header
        assert '#BSUB -M' in cluster.job_header
        assert '#BSUB -q' not in cluster.job_header
        assert '#BSUB -P' not in cluster.job_header

    with LSFCluster(cores=4, memory='8GB',
                    job_extra=['-u email@domain.com']) as cluster:

        assert '#BSUB -u email@domain.com' in cluster.job_header
        assert '#BSUB -n' in cluster.job_header
        assert '#BSUB -W' in cluster.job_header
        assert '#BSUB -M' in cluster.job_header
        assert '#BSUB -q' not in cluster.job_header
        assert '#BSUB -P' not in cluster.job_header


def test_job_script():
    with LSFCluster(walltime='00:02', processes=4, cores=8,
                    memory='8GB') as cluster:

        job_script = cluster.job_script()
        assert '#BSUB' in job_script
        assert '#BSUB -J dask-worker' in job_script
        assert '#BSUB -n 8' in job_script
        assert '#BSUB -M 7630' in job_script
        assert '#BSUB -W 00:02' in job_script
        assert '#BSUB -q' not in cluster.job_header
        assert '#BSUB -P' not in cluster.job_header

        assert '{} -m distributed.cli.dask_worker tcp://'.format(sys.executable) in job_script
        assert '--nthreads 2 --nprocs 4 --memory-limit 2.00GB' in job_script

    with LSFCluster(queue='general', project='DaskOnLSF', processes=4, cores=8,
                    memory='28GB', ncpus=24, mem=100000000000) as cluster:

        job_script = cluster.job_script()
        assert '#BSUB -q general' in cluster.job_header
        assert '#BSUB -J dask-worker' in cluster.job_header
        assert '#BSUB -n 24' in cluster.job_header
        assert '#BSUB -n 8' not in cluster.job_header
        assert '#BSUB -M 95368' in cluster.job_header
        assert '#BSUB -M 26703' not in cluster.job_header
        assert '#BSUB -W' in cluster.job_header
        assert '#BSUB -P DaskOnLSF' in cluster.job_header

        assert '{} -m distributed.cli.dask_worker tcp://'.format(sys.executable) in job_script
        assert '--nthreads 2 --nprocs 4 --memory-limit 7.00GB' in job_script


@pytest.mark.env("lsf")  # noqa: F811
def test_basic(loop):
    with LSFCluster(walltime='00:02', processes=1, cores=2, memory='2GB',
                    local_directory='/tmp', loop=loop) as cluster:

        with Client(cluster) as client:
            workers = cluster.start_workers(2)
            future = client.submit(lambda x: x + 1, 10)
            assert future.result(60) == 11
            assert cluster.jobs

            info = client.scheduler_info()
            w = list(info['workers'].values())[0]
            assert w['memory_limit'] == 2e9
            assert w['ncores'] == 2

            cluster.stop_workers(workers)

            start = time()
            while len(client.scheduler_info()['workers']) > 0:
                sleep(0.100)
                assert time() < start + 10

            assert not cluster.jobs


@pytest.mark.env("lsf")  # noqa: F811
def test_adaptive(loop):
    with LSFCluster(walltime='00:02', processes=1, cores=2, memory='2GB',
                    local_directory='/tmp', loop=loop) as cluster:
        cluster.adapt()
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)
            assert future.result(60) == 11

            assert cluster.jobs

            start = time()
            processes = cluster.worker_processes
            while len(client.scheduler_info()['workers']) != processes:
                sleep(0.1)
                assert time() < start + 10

            del future

            start = time()
            while len(client.scheduler_info()['workers']) > 0:
                sleep(0.100)
                assert time() < start + 10


def test_config(loop):  # noqa: F811
    with dask.config.set({'jobqueue.lsf.walltime': '00:02',
                          'jobqueue.lsf.local-directory': '/foo'}):
        with LSFCluster(loop=loop) as cluster:
            assert '00:02' in cluster.job_script()
            assert '--local-directory /foo' in cluster.job_script()


def test_informative_errors():
    with pytest.raises(ValueError) as info:
        LSFCluster(memory=None, cores=4)
    assert 'memory' in str(info.value)

    with pytest.raises(ValueError) as info:
        LSFCluster(memory='1GB', cores=None)
    assert 'cores' in str(info.value)
