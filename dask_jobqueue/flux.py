import logging
import shlex

import dask

from .core import Job, JobQueueCluster, job_parameters, cluster_parameters

logger = logging.getLogger(__name__)


def _without_arg(command, option):
    command_args = shlex.split(command)
    filtered_args = []
    skip_next = False

    for arg in command_args:
        if skip_next:
            skip_next = False
            continue

        if arg == option:
            skip_next = True
            continue

        filtered_args.append(arg)

    return " ".join(filtered_args)


def _flux_walltime(value):
    if value is None:
        return None

    if isinstance(value, str) and value.count(":") == 2:
        hours, minutes, seconds = value.split(":")
        total_seconds = (int(hours) * 3600) + (int(minutes) * 60) + int(seconds)

        if total_seconds % 3600 == 0:
            return f"{total_seconds // 3600}h"
        if total_seconds % 60 == 0:
            return f"{total_seconds // 60}m"
        return f"{total_seconds}s"

    return value


class FluxJob(Job):
    submit_command = "flux batch"
    cancel_command = "flux cancel"
    config_name = "flux"

    def __init__(
        self,
        scheduler=None,
        name=None,
        queue=None,
        account=None,
        walltime=None,
        job_cpu=None,
        job_nodes=None,
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
        if walltime is None:
            walltime = dask.config.get("jobqueue.%s.walltime" % self.config_name)
        walltime = _flux_walltime(walltime)
        if job_cpu is None:
            job_cpu = dask.config.get("jobqueue.%s.job-cpu" % self.config_name)
        if job_nodes is None:
            job_nodes = dask.config.get("jobqueue.%s.job-nodes" % self.config_name)
        if job_nodes is None:
            job_nodes = 1
        if job_nodes < 1:
            raise ValueError("job_nodes must be at least 1")

        self.job_nodes = job_nodes

        header_lines = []
        if self.job_name is not None:
            header_lines.append("#flux: --job-name=%s" % self.job_name)
        if queue is not None:
            header_lines.append("#flux: -q %s" % queue)
        if account is not None:
            header_lines.append("#flux: -B %s" % account)
        if walltime is not None:
            header_lines.append("#flux: -t %s" % walltime)

        if self.job_nodes > 1:
            header_lines.append("#flux: -N %d" % self.job_nodes)
            header_lines.append("#flux: -n %d" % self.worker_processes)
            if job_cpu is None:
                job_cpu = self.worker_process_threads
        else:
            header_lines.append("#flux: -N 1")
            header_lines.append("#flux: -n 1")
            if job_cpu is None:
                job_cpu = self.worker_cores

        if job_cpu is not None:
            header_lines.append("#flux: -c %d" % job_cpu)

        header_lines = list(
            filter(
                lambda line: not any(skip in line for skip in self.job_directives_skip),
                header_lines,
            )
        )
        header_lines.extend(["#flux: %s" % arg for arg in self.job_extra_directives])
        self.job_header = "\n".join(header_lines)

        logger.debug("Job script: \n %s", self.job_script())

    def job_script(self):
        worker_command = self._command_template
        if self.job_nodes > 1:
            worker_command = "flux run -N {nodes} -n {tasks} {command}".format(
                nodes=self.job_nodes,
                tasks=self.worker_processes,
                command=_without_arg(self._command_template, "--nworkers"),
            )

        pieces = {
            "shebang": self.shebang,
            "job_header": self.job_header,
            "job_script_prologue": "\n".join(filter(None, self._job_script_prologue)),
            "worker_command": worker_command,
            "job_script_epilogue": "\n".join(filter(None, self._job_script_epilogue)),
        }
        return self._script_template % pieces

    def _job_id_from_submit_output(self, out):
        return out.strip()


class FluxCluster(JobQueueCluster):
    __doc__ = """ Launch Dask on a Flux cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to ``flux batch -q``.
    account : str
        Bank associated with each worker job. Passed to ``flux batch -B``.
    {job}
    {cluster}
    walltime : str
        Walltime for each worker job in Flux standard duration syntax.
    memory : str
        Total memory budget exposed to Dask for each submitted job.
        This value is used to derive the Dask worker ``--memory-limit`` setting,
        but Flux itself does not enforce or reserve memory from this parameter.
    job_cpu : int
        Number of CPU cores to allocate per Flux slot.
    job_nodes : int
        Number of nodes to allocate per submitted job.
        For multi-node jobs, one Dask worker process is launched per Flux slot.
    job_extra : list
        Deprecated: use ``job_extra_directives`` instead. This parameter will be removed in a future version.
    job_extra_directives : list
        List of other Flux options. Each option will be prepended with the ``#flux:`` prefix.

    Examples
    --------
    >>> from dask_jobqueue import FluxCluster
    >>> cluster = FluxCluster(queue='pdebug', cores=4, memory="16 GB")
    >>> cluster.scale(jobs=2)
    """.format(
        job=job_parameters, cluster=cluster_parameters
    )
    job_cls = FluxJob
