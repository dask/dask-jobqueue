import pytest

from dask_jobqueue import JobQueueCluster
from dask_jobqueue.core import Job


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

    job.update(worker='worker1')
    job.update(worker='worker2')
    assert job.status == 'running'
    assert len(job.workers) == 2
