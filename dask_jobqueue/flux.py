from contextlib import contextmanager, suppress
import logging
import json
import os
import shlex
import stat
import tempfile
import warnings
import dask

from .core import Job, JobQueueCluster, job_parameters, cluster_parameters

logger = logging.getLogger(__name__)


class FluxJob(Job):
    # Override class variables
    submit_command = "flux batch"
    cancel_command = "flux cancel"
    config_name = "flux"

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

        # This would better be shared logic across backends...
        if queue is None:
            queue = dask.config.get("jobqueue.%s.queue" % self.config_name)
        if project is not None:
            warnings.warn("flux does not have support for project", FutureWarning)
        if account is not None:
            warnings.warn("flux does not have support for accounts", FutureWarning)
        if job_mem is not None:
            warnings.warn("flux does not have support for memory", FutureWarning)

        if walltime is None:
            walltime = dask.config.get("jobqueue.%s.walltime" % self.config_name)
        if job_cpu is None:
            job_cpu = dask.config.get("jobqueue.%s.job-cpu" % self.config_name)

        header_lines = []

        # Flux header build
        if self.job_name is not None:
            header_lines.append("#flux: --job-name=%s" % self.job_name)
        if self.log_directory is not None:
            header_lines.append(
                "#flux: --error %s/%s-%%J.err"
                % (self.log_directory, self.job_name or "worker")
            )
            header_lines.append(
                "#flux: --output %s/%s-%%J.out"
                % (self.log_directory, self.job_name or "worker")
            )
        if queue is not None:
            header_lines.append("#flux: --queue=%s" % queue)

        # Init resources, always 1 task,
        # and then number of cpu is processes * threads if not set
        header_lines.append("#flux: -n 1")
        header_lines.append(
            "#flux: --cores-per-slot=%d" % (job_cpu or self.worker_cores)
        )

        if walltime is not None:
            header_lines.append("#flux: -t %s" % walltime)

        # Skip requested header directives
        header_lines = list(
            filter(
                lambda line: not any(skip in line for skip in self.job_directives_skip),
                header_lines,
            )
        )

        # Add extra header directives
        header_lines.extend(["#flux: %s" % arg for arg in self.job_extra_directives])

        # Declare class attribute that shall be overridden
        self.job_header = "\n".join(header_lines)

    def _job_id_from_submit_output(self, out):
        """The flux jobid looks like ƒBXsRhMtb"""
        return out.strip()

    @classmethod
    def _close_job(cls, job_id, cancel_command):
        """Custom close job to also clean up script file"""
        if job_id:
            # Cancel the job
            with suppress(RuntimeError):  # deleting job when job already gone
                cls._call(shlex.split(cancel_command) + [job_id])
            # Commented out for now - when we delete the job script, there
            # seem to be a number of failed jobs (that continue running after
            # the actual tasks are finished).
            # cls._cleanup_job(job_id)
            logger.debug("Closed job %s", job_id)

    @classmethod
    def _cleanup_job(cls, job_id):
        """Custom cleanup method to remove script file"""
        # Get the original script path from info
        with suppress(RuntimeError):
            raw = cls._call(["flux", "job", "info", "--original", job_id, "jobspec"])
            jobspec = json.loads(raw)
            command = jobspec["tasks"][0]["command"]
            if command and os.path.exists(command[0]):
                logger.debug("Cleaning up %s" % command[0])
                os.remove(command[0])

    @contextmanager
    def job_file(self):
        """Write job submission script to temporary file

        We also want to make this executable and return the full path,
        and not cleanup at the end, as doing so can create a race condition
        between flux reading / starting the job and cleaning it up.
        """
        fd, path = tempfile.mkstemp(prefix="flux-job-", suffix=".sh")
        try:
            logger.debug("writing job script: \n%s", self.job_script())
            with os.fdopen(fd, "w") as f:
                f.write(self.job_script())
        except Exception:
            raise ValueError("Issue writing file %s" % path)

        # Make the file executable, return the full path
        st = os.stat(path)
        os.chmod(path, st.st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        yield os.path.abspath(path)


class FluxCluster(JobQueueCluster):
    __doc__ = """Launch Dask on a Flux cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#SBATCH -p` option.
    project : str
        Deprecated: use ``account`` instead. This parameter will be removed in a future version.
    account : str
        Flux does not have support for an account
    {job}
    {cluster}
    walltime : str
        Walltime for each worker job.
    job_cpu : int
        Number of cpu to book in Flux, if None, defaults to worker `threads * processes`
    job_mem : str
        Flux does not have support to control memory.
    job_extra : list
        Deprecated: use ``job_extra_directives`` instead. This parameter will be removed in a future version.
    job_extra_directives : list
        List of other Flux options, for example -j oe. Each option will be prepended with the #SBATCH prefix.

    Examples
    --------
    >>> from dask_jobqueue import FluxCluster
    >>> cluster = FluxCluster(
    ...     queue='regular',
    ...     cores=24,
    ... )
    >>> cluster.scale(jobs=10)  # ask for 10 jobs

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt(maximum_jobs=20)
    """.format(
        job=job_parameters, cluster=cluster_parameters
    )
    job_cls = FluxJob
