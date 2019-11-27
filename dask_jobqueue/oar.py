import logging
import shlex

import dask

from .core import JobQueueCluster, Job, job_parameters, cluster_parameters

logger = logging.getLogger(__name__)


class OARJob(Job):

    # Override class variables
    submit_command = "oarsub"
    cancel_command = "oardel"
    job_id_regexp = r"OAR_JOB_ID=(?P<job_id>\d+)"
    config_name = "oar"

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

        self.job_header = self.template_env.get_template("oar_job_header").render(
            job_name=self.job_name,
            queue=queue,
            project=project,
            resource_spec=resource_spec,
            walltime=walltime,
            worker_cores=self.worker_cores,
            job_extra=job_extra,
        )

        logger.debug("Job script: \n %s" % self.job_script())

    async def _submit_job(self, fn):
        # OAR specificity: the submission script needs to exist on the worker
        # when the job starts on the worker. This is different from other
        # schedulers that only need the script on the submission node at
        # submission time. That means that we can not use the same strategy as
        # in JobQueueCluster: create a temporary submission file, submit the
        # script, delete the submission file. In order to reuse the code in
        # the base JobQueueCluster class, we read from the temporary file and
        # reconstruct the command line where the script is passed in as a
        # string (inline script in OAR jargon) rather than as a filename.
        with open(fn) as f:
            content_lines = f.readlines()

        oar_lines = [line for line in content_lines if line.startswith("#OAR ")]
        oarsub_options = [line.replace("#OAR ", "").strip() for line in oar_lines]
        inline_script_lines = [
            line for line in content_lines if not line.startswith("#")
        ]
        inline_script = "".join(inline_script_lines)
        oarsub_command = " ".join([self.submit_command] + oarsub_options)
        oarsub_command_split = shlex.split(oarsub_command) + [inline_script]
        return self._call(oarsub_command_split)


class OARCluster(JobQueueCluster):
    __doc__ = """ Launch Dask on an OAR cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#OAR -q` option.
    project : str
        Accounting string associated with each worker job. Passed to `#OAR -p` option.
    {job}
    {cluster}
    resource_spec : str
        Request resources and specify job placement. Passed to `#OAR -l` option.
    walltime : str
        Walltime for each worker job.
    job_extra : list
        List of other OAR options, for example `-t besteffort`. Each option will be prepended with the #OAR prefix.

    Examples
    --------
    >>> from dask_jobqueue import OARCluster
    >>> cluster = OARCluster(queue='regular')
    >>> cluster.scale(jobs=10)  # ask for 10 jobs

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt(maximum_jobs=20)
    """.format(
        job=job_parameters, cluster=cluster_parameters
    )
    job_cls = OARJob
