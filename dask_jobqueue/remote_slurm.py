import logging
from contextlib import contextmanager
from urllib.parse import urljoin

import aiohttp

from .core import cluster_parameters, job_parameters
from .slurm import SLURMCluster, SLURMJob

logger = logging.getLogger(__name__)


class RemoteSLURMJob(SLURMJob):
    def __init__(self, *args, **kwargs):
        self.api_url = kwargs.pop("api_url")
        self.__class__.api_url = self.api_url
        super().__init__(*args, **kwargs)

    @contextmanager
    def job_file(self):
        """
        Override the job_file and pass back the job_script. job_file() is used by Job.start and
         gives output directly to SLURMJob._submit_job().
        """
        yield self.job_script()

    async def _submit_job(self, script):
        # https://slurm.schedmd.com/rest_api.html#slurmctldSubmitJob
        try:
            async with aiohttp.ClientSession() as session:
                response = await session.post(
                    urljoin(self.api_url, "jobs/submit"), json={"script": script}
                )
                response.raise_for_status()
        except aiohttp.ClientError as e:
            logger.exception("SLURMJob request failed.")
            raise RuntimeError from e
        else:
            return response.json()

    def _job_id_from_submit_output(self, out):
        # out is the JSON output from _submit_job request.post
        # See https://slurm.schedmd.com/rest_api.html#v0.0.36_job_submission_response
        return out["job_id"]

    async def close(self):
        logger.debug("Stopping worker: %s job: %s", self.name, self.job_id)
        # https://slurm.schedmd.com/rest_api.html#slurmctldCancelJob
        try:
            async with aiohttp.ClientSession() as session:
                response = await session.delete(
                    urljoin(self.api_url, f"job/{self.job_id}")
                )
                response.raise_for_status()
        except aiohttp.ClientError as e:
            logger.exception("SLURMJob request failed.")
            raise RuntimeError from e
        else:
            return response

    @classmethod
    def _close_job(cls, job_id):
        # Overriding to please weakref in Job.start, not used.
        pass


class RemoteSLURMCluster(SLURMCluster):
    __doc__ = """ Launch Dask on a SLURM cluster

    Parameters
    ----------
    api_url: str
        A url (ending in /) pointing to the SLURM REST API
        See https://slurm.schedmd.com/rest_api.html
    queue : str
        Destination queue for each worker job. Passed to `#SBATCH -p` option.
    project : str
        Accounting string associated with each worker job. Passed to `#SBATCH -A` option.
    {job}
    {cluster}
    walltime : str
        Walltime for each worker job.
    job_cpu : int
        Number of cpu to book in SLURM, if None, defaults to worker `threads * processes`
    job_mem : str
        Amount of memory to request in SLURM. If None, defaults to worker
        processes * memory
    job_extra : list
        List of other Slurm options, for example -j oe. Each option will be prepended with the #SBATCH prefix.

    Examples
    --------
    >>> from dask_jobqueue import SLURMCluster
    >>> cluster = RemoteSLURMCluster(
    ...     api_url='http://a-url.com/slurm/',
    ...     queue='regular',
    ...     project="myproj",
    ...     cores=24,
    ...     memory="500 GB"
    ... )
    >>> cluster.scale(jobs=10)  # ask for 10 jobs

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt(maximum_jobs=20)
    """.format(
        job=job_parameters, cluster=cluster_parameters
    )

    def __init__(self, *args, **kwargs):
        if "api_url" not in kwargs:
            raise ValueError("api_url is missing from RemoteSlurmCluster.")

        super().__init__(*args, **kwargs)

    job_cls = RemoteSLURMJob
