from __future__ import absolute_import, division, print_function

import pytest
import socket

from dask_jobqueue import (JobQueueCluster, PBSCluster, MoabCluster,
                           SLURMCluster, SGECluster, LSFCluster)


def test_errors():
    with pytest.raises(NotImplementedError) as info:
        JobQueueCluster(cores=4)

    assert 'abstract class' in str(info.value)


def test_threads_deprecation():
    with pytest.raises(ValueError) as info:
        JobQueueCluster(threads=4)

    assert all(word in str(info.value)
               for word in ['threads', 'core', 'processes'])


@pytest.mark.parametrize('Cluster', [PBSCluster, MoabCluster, SLURMCluster,
                                     SGECluster, LSFCluster])
def test_repr(Cluster):
    with Cluster(walltime='00:02:00', processes=4, cores=8, memory='28GB',
                 name='dask-worker') as cluster:
        cluster_repr = repr(cluster)
        assert cluster.__class__.__name__ in cluster_repr
        assert 'cores=0' in cluster_repr
        assert 'memory=0 B' in cluster_repr
        assert 'workers=0' in cluster_repr


def test_forward_ip():
    ip = '127.0.0.1'
    with PBSCluster(walltime='00:02:00', processes=4, cores=8, memory='28GB',
                    name='dask-worker', ip=ip) as cluster:
        assert cluster.local_cluster.scheduler.ip == ip

    default_ip = socket.gethostbyname('')
    with PBSCluster(walltime='00:02:00', processes=4, cores=8, memory='28GB',
                    name='dask-worker') as cluster:
        assert cluster.local_cluster.scheduler.ip == default_ip


@pytest.mark.parametrize('qsub_return_string',
                         ['Request {jobid}.asdf was sumbitted to queue 12.',
                          '{jobid}',
                          '  <{jobid}>  ',
                          '{jobid}; asdf'])
def test_jobid_from_qsub(qsub_return_string):
    jobid = '654321'
    qsub_return_string = qsub_return_string.format(jobid)
    assert (JobQueueCluster._job_id_from_submit_output(qsub_return_string) ==
            jobid)
