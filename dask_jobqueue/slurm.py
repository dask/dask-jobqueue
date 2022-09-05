import logging
import math
import warnings

import dask

from .core import Job, JobQueueCluster, job_parameters, cluster_parameters

logger = logging.getLogger(__name__)


class SLURMJob(Job):
    # Override class variables
    submit_command = "sbatch"
    cancel_command = "scancel"
    config_name = "slurm"

    def __init__(
        self,
        scheduler=None,
        name=None,
        queue=None,
        project=None,
        account=None,
        walltime=None,
        job_cpu=None,
        job_mem=None,
        config_name=None,
        **base_class_kwargs
    ):
        super().__init__(
            scheduler=scheduler, name=name, config_name=config_name, **base_class_kwargs
        )

        if queue is None:
            queue = dask.config.get("jobqueue.%s.queue" % self.config_name)
        if account is None:
            account = dask.config.get("jobqueue.%s.account" % self.config_name)
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

        if walltime is None:
            walltime = dask.config.get("jobqueue.%s.walltime" % self.config_name)
        if job_cpu is None:
            job_cpu = dask.config.get("jobqueue.%s.job-cpu" % self.config_name)
        if job_mem is None:
            job_mem = dask.config.get("jobqueue.%s.job-mem" % self.config_name)

        header_lines = []
        # SLURM header build
        if self.job_name is not None:
            header_lines.append("#SBATCH -J %s" % self.job_name)
        if self.log_directory is not None:
            header_lines.append(
                "#SBATCH -e %s/%s-%%J.err"
                % (self.log_directory, self.job_name or "worker")
            )
            header_lines.append(
                "#SBATCH -o %s/%s-%%J.out"
                % (self.log_directory, self.job_name or "worker")
            )
        if queue is not None:
            header_lines.append("#SBATCH -p %s" % queue)
        if account is not None:
            header_lines.append("#SBATCH -A %s" % account)

        # Init resources, always 1 task,
        # and then number of cpu is processes * threads if not set
        header_lines.append("#SBATCH -n 1")
        header_lines.append(
            "#SBATCH --cpus-per-task=%d" % (job_cpu or self.worker_cores)
        )
        # Memory
        memory = job_mem
        if job_mem is None:
            memory = slurm_format_bytes_ceil(self.worker_memory)
        if memory is not None:
            header_lines.append("#SBATCH --mem=%s" % memory)

        if walltime is not None:
            header_lines.append("#SBATCH -t %s" % walltime)

        # Skip requested header directives
        header_lines = list(
            filter(
                lambda line: not any(skip in line for skip in self.job_directives_skip),
                header_lines,
            )
        )

        # Add extra header directives
        header_lines.extend(["#SBATCH %s" % arg for arg in self.job_extra_directives])

        # Declare class attribute that shall be overridden
        self.job_header = "\n".join(header_lines)


def slurm_format_bytes_ceil(n):
    """Format bytes as text.

    SLURM expects KiB, MiB or Gib, but names it KB, MB, GB. SLURM does not handle Bytes, only starts at KB.

    >>> slurm_format_bytes_ceil(1)
    '1K'
    >>> slurm_format_bytes_ceil(1234)
    '2K'
    >>> slurm_format_bytes_ceil(12345678)
    '13M'
    >>> slurm_format_bytes_ceil(1234567890)
    '2G'
    >>> slurm_format_bytes_ceil(15000000000)
    '14G'
    """
    if n >= (1024**3):
        return "%dG" % math.ceil(n / (1024**3))
    if n >= (1024**2):
        return "%dM" % math.ceil(n / (1024**2))
    if n >= 1024:
        return "%dK" % math.ceil(n / 1024)
    return "1K" % n


class SLURMCluster(JobQueueCluster):
    __doc__ = """ Launch Dask on a SLURM cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#SBATCH -p` option.
    project : str
        Deprecated: use ``account`` instead. This parameter will be removed in a future version.
    account : str
        Accounting string associated with each worker job. Passed to `#PBS -A` option.
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
        Deprecated: use ``job_extra_directives`` instead. This parameter will be removed in a future version.
    job_extra_directives : list
        List of other Slurm options, for example -j oe. Each option will be prepended with the #SBATCH prefix.

    Examples
    --------
    >>> from dask_jobqueue import SLURMCluster
    >>> cluster = SLURMCluster(
    ...     queue='regular',
    ...     account="myaccount",
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
    job_cls = SLURMJob
