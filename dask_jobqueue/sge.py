from __future__ import absolute_import, division, print_function

import logging
import os
import math

import dask

from .core import JobQueueCluster, docstrings

logger = logging.getLogger(__name__)


class SGECluster(JobQueueCluster):
    __doc__ = docstrings.with_indents(""" Launch Dask on a SGE cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#$ -q` option.
    project : str
        Accounting string associated with each worker job. Passed to `#$ -A` option.
    resource_spec : str
        Request resources and specify job placement. Passed to `#$ -l` option.
    walltime : str
        Walltime for each worker job.
    %(JobQueueCluster.parameters)s

    Examples
    --------
    >>> from dask_jobqueue import SGECluster
    >>> cluster = SGECluster(queue='regular')
    >>> cluster.scale(10)  # this may take a few seconds to launch

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt()
    """, 4)

    # Override class variables
    submit_command = 'qsub -terse'
    cancel_command = 'qdel'

    def __init__(self, queue=None, project=None, resource_spec=None, walltime=None,
                 config_name='sge', use_job_arrays=True, **kwargs):
        if queue is None:
            queue = dask.config.get('jobqueue.%s.queue' % config_name)
        if project is None:
            project = dask.config.get('jobqueue.%s.project' % config_name)
        if resource_spec is None:
            resource_spec = dask.config.get('jobqueue.%s.resource-spec' % config_name)
        if walltime is None:
            walltime = dask.config.get('jobqueue.%s.walltime' % config_name)

        super(SGECluster, self).__init__(config_name=config_name, **kwargs)

        # Use job arrays?
        # Revert to separate jobs if 'SGE_TASK_ID' is not defined
        self.use_job_arrays = use_job_arrays and bool(os.getenv('SGE_TASK_ID', None))

        header_lines = []
        if self.name is not None:
            header_lines.append('#$ -N %(name)s')
        if queue is not None:
            header_lines.append('#$ -q %(queue)s')
        if project is not None:
            header_lines.append('#$ -P %(project)s')
        if resource_spec is not None:
            header_lines.append('#$ -l %(resource_spec)s')
        if walltime is not None:
            header_lines.append('#$ -l h_rt=%(walltime)s')
        if self.log_directory is not None:
            header_lines.append('#$ -e %(log_directory)s/')
            header_lines.append('#$ -o %(log_directory)s/')
        if self.use_job_arrays:
            header_lines.append('JOB_ID=${JOB_ID}.${SGE_TASK_ID}')
            header_lines.append('#$ -t 1-%(num_jobs_placeholder)s')
        header_lines.extend(['#$ -cwd', '#$ -j y'])
        header_template = '\n'.join(header_lines)

        config = {'name': self.name,
                  'queue': queue,
                  'project': project,
                  'processes': self.worker_processes,
                  'walltime': walltime,
                  'resource_spec': resource_spec,
                  'log_directory': self.log_directory,
                  'num_jobs_placeholder': '%(num_jobs)d'}
        self.job_header = header_template % config

        logger.debug("Job script: \n %s" % self.job_script())

    def start_workers(self, n=1):
        """
        Start workers as a task array

        Parameters
        ----------
        n : int
            Total number of workers to start
        """

        if not self.use_job_arrays:
            logging.debug('submission using job ids')
            return super(SGECluster, self).start_workers(n=n)

        logging.debug('submission using task array')
        logger.debug('starting %s workers', n)

        num_jobs = int(math.ceil(n / self.worker_processes))
        old_job_header = self.job_header
        self.job_header = old_job_header % {'num_jobs': num_jobs}
        with self.job_file() as fn:
            out = self._submit_job(fn)
            job = self._job_id_from_submit_output(out)
            if not job:
                raise ValueError('Unable to parse jobid from output of %s' % out)
            logger.debug("started job: %s", job)
        logger.debug("Adding tasks to list of pending jobs")
        for task in range(1, num_jobs + 1):
            self.pending_jobs['{}.{}'.format(job, task)] = {}
        self.job_header = old_job_header
