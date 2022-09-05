import logging
import math
import os
import warnings

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
    if n >= 10 * (1024**3):
        return "%dGB" % math.ceil(n / (1024**3))
    if n >= 10 * (1024**2):
        return "%dMB" % math.ceil(n / (1024**2))
    if n >= 10 * 1024:
        return "%dkB" % math.ceil(n / 1024)
    return "%dB" % n


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
        account=None,
        resource_spec=None,
        walltime=None,
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

        if account is None:
            account = dask.config.get(
                "jobqueue.%s.account" % self.config_name
            ) or os.environ.get("PBS_ACCOUNT")
        if project is None:
            project = dask.config.get("jobqueue.%s.project" % self.config_name, None)
        if project is not None:
            warn = (
                "project has been renamed to account as this kwarg was used wit -A option. "
                "You are still using it (please also check config files). "
                "If you did not set account yet, project will be respected for now, "
                "but it will be removed in a future release. "
                "If you already set account, project is ignored and you can remove it."
            )
            warnings.warn(warn, FutureWarning)
            if not account:
                account = project

        header_lines = []
        # PBS header build
        if self.job_name is not None:
            header_lines.append("#PBS -N %s" % self.job_name)
        if queue is not None:
            header_lines.append("#PBS -q %s" % queue)
        if account is not None:
            header_lines.append("#PBS -A %s" % account)
        if resource_spec is None:
            # Compute default resources specifications
            resource_spec = "select=1:ncpus=%d" % self.worker_cores
            memory_string = pbs_format_bytes_ceil(self.worker_memory)
            resource_spec += ":mem=" + memory_string
            logger.info(
                "Resource specification for PBS not set, initializing it to %s"
                % resource_spec
            )
        if resource_spec is not None:
            header_lines.append("#PBS -l %s" % resource_spec)
        if walltime is not None:
            header_lines.append("#PBS -l walltime=%s" % walltime)
        if self.log_directory is not None:
            header_lines.append("#PBS -e %s/" % self.log_directory)
            header_lines.append("#PBS -o %s/" % self.log_directory)

        # Skip requested header directives
        header_lines = list(
            filter(
                lambda line: not any(skip in line for skip in self.job_directives_skip),
                header_lines,
            )
        )

        # Add extra header directives
        header_lines.extend(["#PBS %s" % arg for arg in self.job_extra_directives])

        # Declare class attribute that shall be overridden
        self.job_header = "\n".join(header_lines)

        logger.debug("Job script: \n %s" % self.job_script())


class PBSCluster(JobQueueCluster):
    __doc__ = """ Launch Dask on a PBS cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#PBS -q` option.
    project : str
        Deprecated: use ``account`` instead. This parameter will be removed in a future version.
    account : str
        Accounting string associated with each worker job. Passed to `#PBS -A` option.
    {job}
    {cluster}
    resource_spec : str
        Request resources and specify job placement. Passed to `#PBS -l` option.
    walltime : str
        Walltime for each worker job.
    job_extra : list
        Deprecated: use ``job_extra_directives`` instead. This parameter will be removed in a future version.
    job_extra_directives : list
        List of other PBS options. Each option will be prepended with the #PBS prefix.

    Examples
    --------
    >>> from dask_jobqueue import PBSCluster
    >>> cluster = PBSCluster(queue='regular', account="myaccountingstring", cores=24,
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
