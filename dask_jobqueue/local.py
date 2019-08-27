import functools
import logging
import os
import subprocess

from .job import Job, JobQueueCluster

logger = logging.getLogger(__name__)


class LocalJob(Job):
    """ This is mostly used for testing.  It runs locally. """

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
        self.process.stderr.readline()  # make sure that we start
        return str(self.process.pid)

    @classmethod
    def _close_job(self, job_id):
        os.kill(int(job_id), 9)
        # from distributed.utils_test import terminate_process
        # terminate_process(self.process)


LocalCluster = functools.partial(JobQueueCluster, Job=LocalJob, config_name="local")
