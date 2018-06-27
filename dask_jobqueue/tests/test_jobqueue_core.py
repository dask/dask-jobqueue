import pytest

from dask_jobqueue import JobQueueCluster


def test_errors():
    with pytest.raises(NotImplementedError) as info:
        JobQueueCluster()

    assert 'abstract class' in str(info.value)
