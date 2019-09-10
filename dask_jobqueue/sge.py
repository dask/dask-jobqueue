import logging

import dask

from .job import Job, JobQueueCluster, job_parameters, cluster_parameters

logger = logging.getLogger(__name__)


class SGEJob(Job):
    submit_command = "qsub"
    cancel_command = "qdel"

    def __init__(
        self,
        *args,
        queue=None,
        project=None,
        resource_spec=None,
        walltime=None,
        job_extra=None,
        config_name="sge",
        **kwargs
    ):
        if queue is None:
            queue = dask.config.get("jobqueue.%s.queue" % config_name)
        if project is None:
            project = dask.config.get("jobqueue.%s.project" % config_name)
        if resource_spec is None:
            resource_spec = dask.config.get("jobqueue.%s.resource-spec" % config_name)
        if walltime is None:
            walltime = dask.config.get("jobqueue.%s.walltime" % config_name)
        if job_extra is None:
            job_extra = dask.config.get("jobqueue.%s.job-extra" % config_name)

        super().__init__(*args, config_name=config_name, **kwargs)

        header_lines = []
        if self.job_name is not None:
            header_lines.append("#$ -N %(job-name)s")
        if queue is not None:
            header_lines.append("#$ -q %(queue)s")
        if project is not None:
            header_lines.append("#$ -P %(project)s")
        if resource_spec is not None:
            header_lines.append("#$ -l %(resource_spec)s")
        if walltime is not None:
            header_lines.append("#$ -l h_rt=%(walltime)s")
        if self.log_directory is not None:
            header_lines.append("#$ -e %(log_directory)s/")
            header_lines.append("#$ -o %(log_directory)s/")
        header_lines.extend(["#$ -cwd", "#$ -j y"])
        header_lines.extend(["#$ %s" % arg for arg in job_extra])
        header_template = "\n".join(header_lines)

        config = {
            "job-name": self.job_name,
            "queue": queue,
            "project": project,
            "processes": self.worker_processes,
            "walltime": walltime,
            "resource_spec": resource_spec,
            "log_directory": self.log_directory,
        }
        self.job_header = header_template % config

        logger.debug("Job script: \n %s" % self.job_script())


class SGECluster(JobQueueCluster):
    __doc__ = """ Launch Dask on an SGE cluster

    .. note::
        If you want a specific amount of RAM, both ``memory`` and ``resource_spec``
        must be specified. The exact syntax of ``resource_spec`` is defined by your
        GridEngine system administrator. The amount of ``memory`` requested should
        match the ``resource_spec``, so that Dask's memory management system can
        perform accurately.

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#$ -q` option.
    project : str
        Accounting string associated with each worker job. Passed to `#$ -A` option.

    {job}

    {cluster}

    resource_spec : str
        Request resources and specify job placement. Passed to `#$ -l` option.
    walltime : str
        Walltime for each worker job.
    job_extra : list
        List of other SGE options, for example -w e. Each option will be
        prepended with the #$ prefix.

    Examples
    --------
    >>> from dask_jobqueue import SGECluster
    >>> cluster = SGECluster(
    ...     queue='regular',
    ...     project="myproj",
    ...     cores=24,
    ...     memory="500 GB"
    ... )
    >>> cluster.scale(10)  # this may take a few seconds to launch

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt()
    """.format(
        job=job_parameters, cluster=cluster_parameters
    )
    Job = SGEJob
    config_name = "sge"
