import pytest

from dask_jobqueue import JobQueueCluster
from dask_jobqueue.core import Job


class TestWorker(object):
    def __init__(self, name, address):
        self.name = name
        self.address = address


def test_errors():
    with pytest.raises(NotImplementedError) as info:
        JobQueueCluster()

    assert 'abstract class' in str(info.value)


def test_job_update():
    job = Job(job_id='1234', status='pending')
    assert job.job_id == '1234'
    assert job.status == 'pending'
    assert len(job.workers) == 0

    job.update(status='running')
    assert job.status == 'running'
    assert len(job.workers) == 0

    job.update(worker=TestWorker('worker1', '127.0.0.1:1234'))
    job.update(worker=TestWorker('worker2', '127.0.0.1:1235'))
    assert job.status == 'running'
    assert len(job.workers) == 2
