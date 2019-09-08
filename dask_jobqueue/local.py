import logging
import os
import subprocess

from .job import Job, JobQueueCluster, job_parameters, cluster_parameters

logger = logging.getLogger(__name__)


class LocalJob(Job):
    __doc__ = """ Use Dask Jobqueue with local bash commands

    This is mostly for testing.  It uses all the same machinery of
    dask-jobqueue, but rather than submitting jobs to some external job
    queueing system, it launches them locally.  For normal local use, please
    see ``dask.distributed.LocalCluster``

    Parameters
    ----------
    {job}
    """.format(
        job=job_parameters
    )

    config_name = "local"

    def __init__(
        self,
        *args,
        queue=None,
        project=None,
        resource_spec=None,
        walltime=None,
        job_extra=None,
        config_name="local",
        **kwargs
    ):
        # Instantiate args and parameters from parent abstract class
        super().__init__(*args, config_name=config_name, shebang="", **kwargs)

        # Declare class attribute that shall be overridden
        header_lines = []
        self.job_header = "\n".join(header_lines)

        logger.debug("Job script: \n %s" % self.job_script())

    def _submit_job(self, script_filename):
        # Should we make this async friendly?
        with open(script_filename) as f:
            text = f.read().strip().split()
        self.process = subprocess.Popen(
            text, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        # TODO this should raise if self.process.returncode != 0. Refactor
        # Job._call to be able to return process (so that we can return self.process.pid below)

        self.process.stderr.readline()  # make sure that we start
        return str(self.process.pid)

    @classmethod
    def _close_job(self, job_id):
        os.kill(int(job_id), 9)
        # from distributed.utils_test import terminate_process
        # terminate_process(self.process)


class LocalCluster(JobQueueCluster):
    __doc__ = """ Use dask-jobqueue with local bash commands

    This is mostly for testing.  It uses all the same machinery of
    dask-jobqueue, but rather than submitting jobs to some external job
    queueing system, it launches them locally.  For normal local use, please
    see ``dask.distributed.LocalCluster``

    Parameters
    ----------
    {job}

    {cluster}

    Examples
    --------
    >>> from dask_jobqueue import LocalCluster
    >>> cluster = LocalCluster(cores=2, memory="4 GB")
    >>> cluster.scale(3)

    See Also
    --------
    dask.distributed.LocalCluster
    """.format(
        job=job_parameters, cluster=cluster_parameters
    )
    Job = LocalJob
    config_name = "local"
