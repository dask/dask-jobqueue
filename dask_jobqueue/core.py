from __future__ import absolute_import, division, print_function

import logging
import math
import os
import shlex
import socket
import subprocess
import sys
from contextlib import contextmanager

import dask
import docrep
from distributed import LocalCluster
from distributed.deploy import Cluster
from distributed.utils import get_ip_interface, ignoring, parse_bytes, tmpfile

dirname = os.path.dirname(sys.executable)

logger = logging.getLogger(__name__)
docstrings = docrep.DocstringProcessor()


@docstrings.get_sectionsf('JobQueueCluster')
class JobQueueCluster(Cluster):
    """ Base class to launch Dask Clusters for Job queues

    This class should not be used directly, use inherited class appropriate
    for your queueing system (e.g. PBScluster or SLURMCluster)

    Parameters
    ----------
    name : str
        Name of Dask workers.
    threads : int
        Number of threads per process.
    processes : int
        Number of processes per node.
    memory : str
        Bytes of memory that the worker can use. This should be a string
        like "7GB" that can be interpretted both by PBS and Dask.
    interface : str
        Network interface like 'eth0' or 'ib0'.
    death_timeout : float
        Seconds to wait for a scheduler before closing workers
    local_directory : str
        Dask worker local directory for file spilling.
    extra : str
        Additional arguments to pass to `dask-worker`
    env_extra : list
        Other commands to add to script before launching worker.
    kwargs : dict
        Additional keyword arguments to pass to `LocalCluster`

    Attributes
    ----------
    submit_command: str
        Abstract attribute for job scheduler submit command,
        should be overriden
    cancel_command: str
        Abstract attribute for job scheduler cancel command,
        should be overriden

    See Also
    --------
    PBSCluster
    SLURMCluster
    """

    _script_template = """
#!/bin/bash

%(job_header)s

%(env_header)s

%(worker_command)s
""".lstrip()

    # Following class attributes should be overriden by extending classes.
    submit_command = None
    cancel_command = None
    _adaptive_options = {
        'worker_key': lambda ws: _job_id_from_worker_name(ws.name)}

    def __init__(self,
                 name=dask.config.get('jobqueue.name'),
                 threads=dask.config.get('jobqueue.threads'),
                 processes=dask.config.get('jobqueue.processes'),
                 memory=dask.config.get('jobqueue.memory'),
                 interface=dask.config.get('jobqueue.interface'),
                 death_timeout=dask.config.get('jobqueue.death-timeout'),
                 local_directory=dask.config.get('jobqueue.local-directory'),
                 extra=dask.config.get('jobqueue.extra'),
                 env_extra=dask.config.get('jobqueue.env-extra'),
                 **kwargs
                 ):
        """ """
        # """
        # This initializer should be considered as Abstract, and never used
        # directly.
        # """
        if not self.cancel_command or not self.submit_command:
            raise NotImplementedError('JobQueueCluster is an abstract class '
                                      'that should not be instanciated.')

        # This attribute should be overriden
        self.job_header = None

        if interface:
            host = get_ip_interface(interface)
            extra += ' --interface  %s ' % interface
        else:
            host = socket.gethostname()

        self.cluster = LocalCluster(n_workers=0, ip=host, **kwargs)

        # Keep information on process, threads and memory, for use in
        # subclasses
        self.worker_memory = parse_bytes(memory)
        self.worker_processes = processes
        self.worker_threads = threads
        self.name = name

        self.n = 0
        self._adaptive = None

        self._env_header = '\n'.join(env_extra)

        # dask-worker command line build
        self._command_template = os.path.join(
            dirname, 'dask-worker %s' % self.scheduler.address)
        if threads is not None:
            self._command_template += " --nthreads %d" % threads
        if processes is not None:
            self._command_template += " --nprocs %d" % processes
        if memory is not None:
            self._command_template += " --memory-limit %s" % memory
        if name is not None:
            # worker names follow this template: {NAME}-{JOB_ID}-{WORKER_NUM}
            self._command_template += " --name %s" % name  # e.g. "dask-worker"
            # Keep %(n) to be replaced later (worker id on this job)
            # ${JOB_ID} is an environment variable describing this job
            self._command_template += "-${JOB_ID}-%(n)d"
        if death_timeout is not None:
            self._command_template += " --death-timeout %s" % death_timeout
        if local_directory is not None:
            self._command_template += " --local-directory %s" % local_directory
        if extra is not None:
            self._command_template += extra

    def job_script(self):
        """ Construct a job submission script """
        self.n += 1
        template = self._command_template % {'n': self.n}
        return self._script_template % {'job_header': self.job_header,
                                        'env_header': self._env_header,
                                        'worker_command': template}

    @contextmanager
    def job_file(self):
        """ Write job submission script to temporary file """
        with tmpfile(extension='sh') as fn:
            with open(fn, 'w') as f:
                f.write(self.job_script())
            yield fn

    def start_workers(self, n=1):
        """ Start workers and point them to our local scheduler """
        job_ids = []
        num_jobs = math.ceil(n / self.worker_processes)
        for _ in range(num_jobs):
            with self.job_file() as fn:
                out = self._call(shlex.split(self.submit_command) + [fn])
                job = self._job_id_from_submit_output(out.decode())
                job_ids.append(job)
        return job_ids

    @property
    def scheduler(self):
        """ The scheduler of this cluster """
        return self.cluster.scheduler

    def _calls(self, cmds):
        """ Call a command using subprocess.communicate

        This centralzies calls out to the command line, providing consistent
        outputs, logging, and an opportunity to go asynchronous in the future

        Parameters
        ----------
        cmd: List(List(str))
            A list of commands, each of which is a list of strings to hand to
            subprocess.communicate

        Examples
        --------
        >>> self._calls([['ls'], ['ls', '/foo']])

        Returns
        -------
        The stdout result as a string
        Also logs any stderr information
        """
        logger.debug("Submitting the following calls to command line")
        procs = []
        for cmd in cmds:
            logger.debug(' '.join(cmd))
            procs.append(subprocess.Popen(cmd,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE))

        result = []
        for proc in procs:
            out, err = proc.communicate()
            if err:
                logger.error(err.decode())
            result.append(out)
        return result

    def _call(self, cmd):
        """ Singular version of _calls """
        return self._calls([cmd])[0]

    def stop_workers(self, workers):
        """ Stop a list of workers"""
        if not workers:
            return
        jobs = {_job_id_from_worker_name(w.name) for w in workers}
        self._call([self.cancel_command] + list(jobs))

    def scale_up(self, n, **kwargs):
        """ Brings total worker count up to ``n`` """
        return self.start_workers(n - len(self.scheduler.workers))

    def scale_down(self, workers):
        ''' Close the workers with the given addresses '''
        workers = []
        for w in workers:
            try:
                # Get the actual "Worker"
                workers.append(self.scheduler.workers[w])
            except KeyError:
                logger.debug('worker %s is already gone' % w)
        self.stop_workers(workers)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stop_workers(self.scheduler.workers)
        self.cluster.__exit__(type, value, traceback)

    def _job_id_from_submit_output(self, out):
        raise NotImplementedError('_job_id_from_submit_output must be '
                                  'implemented when JobQueueCluster is '
                                  'inherited. It should convert the stdout '
                                  'from submit_command to the job id')


def _job_id_from_worker_name(name):
    ''' utility to parse the job ID from the worker name'''
    return name.split('-')[-2]
