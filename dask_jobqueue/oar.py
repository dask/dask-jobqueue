import logging
import shlex
import warnings
import math

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
        memory_per_core_property_name=None,
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

        header_lines = []
        if self.job_name is not None:
            header_lines.append("#OAR -n %s" % self.job_name)
        if queue is not None:
            header_lines.append("#OAR -q %s" % queue)
        if project is not None:
            header_lines.append("#OAR --project %s" % project)

        # OAR needs to have the resource on a single line otherwise it is
        # considered as a "moldable job" (i.e. the scheduler can chose between
        # multiple sets of resources constraints)
        resource_spec_list = []
        if resource_spec is None:
            # default resource_spec if not specified. Crucial to specify
            # nodes=1 to make sure the cores allocated are on the same node.
            resource_spec = "/nodes=1/core=%d" % self.worker_cores
        resource_spec_list.append(resource_spec)
        if walltime is not None:
            resource_spec_list.append("walltime=%s" % walltime)

        full_resource_spec = ",".join(resource_spec_list)
        header_lines.append("#OAR -l %s" % full_resource_spec)

        # Skip requested header directives
        header_lines = list(
            filter(
                lambda line: not any(skip in line for skip in self.job_directives_skip),
                header_lines,
            )
        )

        # Add extra header directives
        header_lines.extend(["#OAR %s" % arg for arg in self.job_extra_directives])

        # Memory
        if self.worker_memory is not None:
            if memory_per_core_property_name is None:
                memory_per_core_property_name = dask.config.get(
                    "jobqueue.%s.memory-per-core-property-name" % self.config_name
                )
            if memory_per_core_property_name is None:
                warn = (
                    "OAR Job memory reserved resources will not be set according to Dask Worker memory limit, "
                    "which can cause crashes. See `memory_per_core_property_name` documentation for more details"
                )
                warnings.warn(warn, category=UserWarning)
            else:
                if memory_per_core_property_name != "not_applicable":
                    # OAR expects MiB as memory unit
                    oar_memory = math.ceil(
                        self.worker_memory / (self.worker_cores * 2**20)
                    )
                    # OAR needs to have the properties on a single line, with SQL syntax
                    # If there are several "#OAR -p" lines, only the last one will be taken into account by OAR
                    last_job_property = return_last_job_property(
                        self.job_extra_directives
                    )
                    if last_job_property is not None:
                        header_lines.append(
                            "#OAR -p '%s AND %s>=%s'"
                            % (
                                last_job_property,
                                memory_per_core_property_name,
                                oar_memory,
                            )
                        )
                    else:
                        header_lines.append(
                            "#OAR -p %s>=%s"
                            % (memory_per_core_property_name, oar_memory)
                        )

        self.job_header = "\n".join(header_lines)

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
        Project associated with each worker job. Passed to `#OAR --project` option.
    {job}
    {cluster}
    resource_spec : str
        Request resources and specify job placement. Passed to `#OAR -l` option.
    walltime : str
        Walltime for each worker job.
    job_extra : list
        Deprecated: use ``job_extra_directives`` instead. This parameter will
        be removed in a future version.
    job_extra_directives : list
        List of other OAR options, for example `-t besteffort`. Each option
        will be prepended with the #OAR prefix.
    memory_per_core_property_name : str
        The memory per core property name of your OAR cluster (usually
        `memcore` or `mem_core`).
        Existing properties can be listed by executing the `oarnodes` command.
        Note that the memory per core property might not exist on your cluster.
        If that is the case, you can set
        `memory_per_core_property_name="not_applicable"` to avoid getting a warning.
        If you leave `memory_per_core_property_name` set to its default value,
        you will get a warning.
        `memory_per_core_property_name` is `"not_applicable"` or set to its
        default value, allocated nodes may not have enough memory to match the
        `memory` parameter and Dask worker memory management may not work
        correctly.

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


def return_last_job_property(job_extra_directives):
    for directive in reversed(job_extra_directives):
        if directive.startswith("-p"):
            return directive.replace("-p ", "")
    return None
