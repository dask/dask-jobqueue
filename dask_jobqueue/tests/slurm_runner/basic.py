from dask.distributed import Client
from dask_jobqueue.slurm import SLURMRunner

with SLURMRunner(scheduler_file="/shared_space/{job_id}.json") as runner:
    with Client(runner) as client:
        assert client.submit(lambda x: x + 1, 10).result() == 11
        assert client.submit(lambda x: x + 1, 20, workers=2).result() == 21
        print("Test passed")
