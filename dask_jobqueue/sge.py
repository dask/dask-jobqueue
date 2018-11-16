from __future__ import absolute_import, division, print_function

import logging

import dask

from .core import JobQueueCluster, docstrings

logger = logging.getLogger(__name__)


class SGECluster(JobQueueCluster):
    __doc__ = docstrings.with_indents(""" Launch Dask on a SGE cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#$ -q` option.
    memory: str
        The amount of memory requested per worker job. Passed to `#$ -l` option. Can be
        specified in lieu of `resource_spec`.
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
    scheduler_name = 'sge'

    def __init__(self, queue=None, project=None, resource_spec=None, walltime=None, **kwargs):
        if queue is None:
            queue = dask.config.get('jobqueue.%s.queue' % self.scheduler_name)
        if project is None:
            project = dask.config.get('jobqueue.%s.project' % self.scheduler_name)
        if resource_spec is None:
            resource_spec = dask.config.get('jobqueue.%s.resource-spec' % self.scheduler_name)
        if walltime is None:
            walltime = dask.config.get('jobqueue.%s.walltime' % self.scheduler_name)

        super(SGECluster, self).__init__(**kwargs)

        header_lines = []
        if self.name is not None:
            header_lines.append('#$ -N %(name)s')
        if queue is not None:
            header_lines.append('#$ -q %(queue)s')
        if project is not None:
            header_lines.append('#$ -P %(project)s')

        # Define resource specifications. There are only two cases allowed in this
        # implementation: either specify using resource_spec, or specify using the rest of the
        # kwargs.
        if resource_spec is not None:
            header_lines.append('#$ -l %(resource_spec)s')
        else:
            header_lines.append('#$ -l m_mem_free=%s' % parse_memory(kwargs.get('memory')))
            logger.info("Resource specification not set for SGE, initializing to %s")

        if walltime is not None:
            header_lines.append('#$ -l h_rt=%(walltime)s')
        if self.log_directory is not None:
            header_lines.append('#$ -e %(log_directory)s/')
            header_lines.append('#$ -o %(log_directory)s/')
        header_lines.extend(['#$ -cwd', '#$ -j y'])
        header_template = '\n'.join(header_lines)

        config = {'name': self.name,
                  'queue': queue,
                  'project': project,
                  'processes': self.worker_processes,
                  'walltime': walltime,
                  'resource_spec': resource_spec,
                  'log_directory': self.log_directory}
        self.job_header = header_template % config

        logger.debug("Job script: \n %s" % self.job_script())


def parse_memory(memory: str):
    """
    Parses the memory string to format it correctly for SGE job scripts.

    Simply uses a bunch of heuristics to parse the string.

    :param memory: The amount of RAM requested per worker node.
    :returns: memory, properly formatted
    """
    # Covering only the most common use cases here. The pattern should be obvious if we need to
    # add more cases below.
    mappings = {
        'tb': 'T',
        't': 'T',
        'gb': 'G',
        'g': 'G',
        'mb': 'M',
        'm': 'M',
        'kb': 'K',
        'k': 'K'
    }
    for ending, replacement in mappings.items():
        if memory.lower().endswith(ending):
            memory = memory.replace(ending, replacement)

    return memory
