from __future__ import absolute_import, division, print_function

import pytest
import socket

from dask_jobqueue import (JobQueueCluster, PBSCluster, MoabCluster,
                           SLURMCluster, SGECluster, LSFCluster)
from dask_jobqueue.core import ClusterStatus


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


def test_status():
    with PBSCluster(walltime='00:02:00', processes=4, cores=8, memory='28GB',
                    name='dask-worker') as cluster:
        status = cluster.status()
        assert isinstance(status, ClusterStatus)
        assert 'Running: (cores=0, memory=0 B, workers=0, jobs=0)' in status.__repr__()
        assert 'Pending: ' in status.__repr__()
        assert 'Finished: ' in status.__repr__()
        assert '<h3>Cluster Status</h3>' in status._repr_html_()
        assert '<b>Running: </b>(cores=0, memory=0 B, workers=0, jobs=0)' in status._repr_html_()

        status = cluster.status(True)
        assert isinstance(status, ClusterStatus)
        assert 'Running: ' in status.__repr__()
        assert 'Pending: ' in status.__repr__()
        assert 'Finished: ' in status.__repr__()
        assert '<h3>Cluster Status</h3>' in status._repr_html_()
        assert '<b>Running: </b>(cores=0, memory=0 B, workers=0, jobs=0)' in status._repr_html_()
