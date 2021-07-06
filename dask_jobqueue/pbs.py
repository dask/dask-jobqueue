import logging
import math
import os

import dask

from .core import Job, JobQueueCluster, job_parameters, cluster_parameters

logger = logging.getLogger(__name__)


def pbs_format_bytes_ceil(n):
    """Format bytes as text.

    PBS expects KiB, MiB or Gib, but names it KB, MB, GB whereas Dask makes the difference between KB and KiB.

    >>> pbs_format_bytes_ceil(1)
    '1B'
    >>> pbs_format_bytes_ceil(1234)
    '1234B'
    >>> pbs_format_bytes_ceil(12345678)
    '13MB'
    >>> pbs_format_bytes_ceil(1234567890)
    '1177MB'
    >>> pbs_format_bytes_ceil(15000000000)
    '14GB'
    """
    if n >= 10 * (1024 ** 3):
        return "%dGB" % math.ceil(n / (1024 ** 3))
    if n >= 10 * (1024 ** 2):
        return "%dMB" % math.ceil(n / (1024 ** 2))
    if n >= 10 * 1024:
        return "%dkB" % math.ceil(n / 1024)
    return "%dB" % n


def pbs_format_resource_spec(resource_spec, worker_cores, worker_memory):
    if resource_spec is None:
        # Compute default resources specifications
        resource_spec = "select=1:ncpus=%d" % worker_cores
        memory_string = pbs_format_bytes_ceil(worker_memory)
        resource_spec += ":mem=" + memory_string
        logger.info(
            "Resource specification for PBS not set, initializing it to %s"
            % resource_spec
        )
    return resource_spec


class PBSJob(Job):
    submit_command = "qsub"
    cancel_command = "qdel"
    config_name = "pbs"

    def __init__(
        self,
        scheduler=None,
        name=None,
        queue=None,
        project=None,
        resource_spec=None,
        walltime=None,
        job_extra=None,
        config_name=None,
        **base_class_kwargs
    ):
        super().__init__(
            scheduler=scheduler, name=name, config_name=config_name, **base_class_kwargs
        )

        if queue is None:
            queue = dask.config.get("jobqueue.%s.queue" % self.config_name)
        if resource_spec is None:
            resource_spec = dask.config.get(
                "jobqueue.%s.resource-spec" % self.config_name
            )
        if walltime is None:
            walltime = dask.config.get("jobqueue.%s.walltime" % self.config_name)
        if job_extra is None:
            job_extra = dask.config.get("jobqueue.%s.job-extra" % self.config_name)
        if project is None:
            project = dask.config.get(
                "jobqueue.%s.project" % self.config_name
            ) or os.environ.get("PBS_ACCOUNT")

        # Try to find a project name from environment variable
        project = project or os.environ.get("PBS_ACCOUNT")

        self.job_header = self.template_env.get_template("pbs_job_header.j2").render(
            job_name=self.job_name,
            queue=queue,
            project=project,
            resource_spec=resource_spec,
            worker_cores=self.worker_cores,
            worker_memory=self.worker_memory,
            walltime=walltime,
            log_directory=self.log_directory,
            job_extra=job_extra,
        )

        logger.debug("Job script: \n %s" % self.job_script())

    @property
    def template_env(self):
        env = super().template_env
        env.filters["pbs_format_resource_spec"] = pbs_format_resource_spec
        return env


class PBSCluster(JobQueueCluster):
    __doc__ = """ Launch Dask on a PBS cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#PBS -q` option.
    project : str
        Accounting string associated with each worker job. Passed to `#PBS -A` option.
    {job}
    {cluster}
    resource_spec : str
        Request resources and specify job placement. Passed to `#PBS -l` option.
    walltime : str
        Walltime for each worker job.
    job_extra : list
        List of other PBS options. Each option will be prepended with the #PBS prefix.

    Examples
    --------
    >>> from dask_jobqueue import PBSCluster
    >>> cluster = PBSCluster(queue='regular', project="myproj", cores=24,
    ...     memory="500 GB")
    >>> cluster.scale(jobs=10)  # ask for 10 jobs

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt(maximum_jobs=20)
    """.format(
        job=job_parameters, cluster=cluster_parameters
    )
    job_cls = PBSJob
