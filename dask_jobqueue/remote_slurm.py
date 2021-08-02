import logging
from contextlib import contextmanager
from typing import Any, Dict

import aiohttp

from .core import cluster_parameters, job_parameters
from .slurm import SLURMCluster, SLURMJob, slurm_format_bytes_ceil

logger = logging.getLogger(__name__)


class RemoteSLURMJob(SLURMJob):
    def __init__(self, *args, **kwargs):
        self.api_client_session_kwargs = kwargs.pop("api_client_session_kwargs")
        self.api_url = kwargs.pop("api_url")
        self.remote_job_extra = kwargs.pop("remote_job_extra", {})
        super().__init__(*args, **kwargs)

    @contextmanager
    def job_file(self):
        """
        Override the job_file and pass back the job_script. job_file() is used by Job.start and
         gives output directly to SLURMJob._submit_job().
        """
        yield self.job_script()

    @property
    def _job_configuration(self) -> Dict:
        configuration = {
            "environment": {"name": "test"},
            "tasks": 1,
            "cpus_per_task": self.job_cpu or self.worker_cores,
        }

        if self.job_name is not None:
            configuration["name"] = self.job_name
        if self.log_directory is not None:
            configuration[
                "standard_error"
            ] = f"{self.log_directory}/{self.job_name or 'worker'}-%%J.err"
            configuration[
                "standard_output"
            ] = f"{self.log_directory}/{self.job_name or 'worker'}-%%J.out"
        if self.queue is not None:
            configuration["partition"] = self.queue
        if self.project is not None:
            configuration["account"] = self.project

        memory = self.job_mem
        if self.job_mem is None:
            memory = slurm_format_bytes_ceil(self.worker_memory)
        if memory is not None:
            configuration["memory_per_node"] = memory

        if self.walltime is not None:
            configuration["time_limit"] = self.walltime

        configuration.update(self.remote_job_extra)

        return configuration

    async def _submit_job(self, script):
        # https://slurm.schedmd.com/rest_api.html#slurmctldSubmitJob
        async with aiohttp.ClientSession(
            raise_for_status=True, **self.api_client_session_kwargs
        ) as client_session:
            # Unfortunately for certain message (like Authentication Failure) SLURM
            # will respond with a 'Content-Type' of application/json but then the response will
            # not be parsable JSON.
            # This now only works if we set `AIOHTTP_NO_EXTENSIONS=1`
            # See https://github.com/aio-libs/aiohttp/issues/1843
            response = await client_session.post(
                f"{self.api_url}slurm/v0.0.36/job/submit",
                json={"script": script, "job": self._job_configuration},
            )
            async with response:
                return await response.json()

    def _job_id_from_submit_output(self, out):
        # out is the JSON output from _submit_job request.post
        # See https://slurm.schedmd.com/rest_api.html#v0.0.36_job_submission_response
        return out["job_id"]

    async def close(self):
        logger.debug("Stopping worker: %s job: %s", self.name, self.job_id)
        # https://slurm.schedmd.com/rest_api.html#slurmctldCancelJob
        async with aiohttp.ClientSession(
            raise_for_status=True, **self.api_client_session_kwargs
        ) as client_session:
            response = await client_session.delete(
                f"{self.api_url}slurm/v0.0.36/job/{self.job_id}",
            )
            async with response:
                return await response.json()

    @classmethod
    def _close_job(cls, job_id):
        # Overriding to please weakref in Job.start, not used.
        pass


class RemoteSLURMCluster(SLURMCluster):
    __doc__ = """ Launch Dask on a SLURM cluster

    Parameters
    ----------
    api_client_session_kwargs: Dict
        A dictionary with key/values for aiohttp.ClientSession. to set up a client session
         with specific headers or connectors.
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
    ...     api_client_session_kwargs=dict(
                connector=aiohttp.UnixConnector(path='/path/to/slurmrestd.socket')
            ),
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

    def __init__(
        self,
        api_client_session_kwargs: Dict[str, Any],
        *args,
        **kwargs,
    ):
        # Set into kwargs, they'll be passed through `**job_kwargs` into RemoteSLURMJob
        kwargs["api_client_session_kwargs"] = api_client_session_kwargs
        super().__init__(*args, **kwargs)

    @classmethod
    def with_socket_api_client(
        cls, socket_api_path: str, *args, **kwargs
    ) -> "RemoteSLURMCluster":
        kwargs["api_url"] = "http://localhost/"
        return cls(
            api_client_session_kwargs=dict(
                connector=aiohttp.UnixConnector(path=socket_api_path)
            ),
            *args,
            **kwargs,
        )

    @classmethod
    def with_http_api_client(
        cls,
        http_api_url: str,
        http_api_user_name: str,
        http_api_user_token: str,
        *args,
        **kwargs,
    ) -> "RemoteSLURMCluster":
        kwargs["api_url"] = (
            http_api_url if http_api_url.endswith("/") else f"{http_api_url}/"
        )
        return cls(
            api_client_session_kwargs=dict(
                headers={
                    "X-SLURM-USER-NAME": http_api_user_name,
                    "X-SLURM-USER-TOKEN": http_api_user_token,
                }
            ),
            *args,
            **kwargs,
        )

    job_cls = RemoteSLURMJob
