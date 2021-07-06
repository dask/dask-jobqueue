import logging

import dask

from .core import Job, JobQueueCluster, job_parameters, cluster_parameters

logger = logging.getLogger(__name__)


class SGEJob(Job):
    submit_command = "qsub"
    cancel_command = "qdel"
    config_name = "sge"

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
        if project is None:
            project = dask.config.get("jobqueue.%s.project" % self.config_name)
        if resource_spec is None:
            resource_spec = dask.config.get(
                "jobqueue.%s.resource-spec" % self.config_name
            )
        if walltime is None:
            walltime = dask.config.get("jobqueue.%s.walltime" % self.config_name)
        if job_extra is None:
            job_extra = dask.config.get("jobqueue.%s.job-extra" % self.config_name)

        self.job_header = self.template_env.get_template("sge_job_header.j2").render(
            job_name=self.job_name,
            queue=queue,
            project=project,
            walltime=walltime,
            resource_spec=resource_spec,
            log_directory=self.log_directory,
            job_extra=job_extra,
        )

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
    >>> cluster.scale(jobs=10)  # ask for 10 jobs

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt(maximum_jobs=20)
    """.format(
        job=job_parameters, cluster=cluster_parameters
    )
    job_cls = SGEJob
