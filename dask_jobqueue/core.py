from __future__ import absolute_import, division, print_function

import logging
import math
import shlex
import socket
import subprocess
import sys
from collections import OrderedDict
from contextlib import contextmanager

import dask
import docrep
from distributed import LocalCluster
from distributed.deploy import Cluster
from distributed.utils import get_ip_interface, parse_bytes, tmpfile
from distributed.diagnostics.plugin import SchedulerPlugin

logger = logging.getLogger(__name__)
docstrings = docrep.DocstringProcessor()


def _job_id_from_worker_name(name):
    ''' utility to parse the job ID from the worker name

    template: 'prefix-jobid[-proc]'
    '''
    pieces = name.split('-')
    print(name, pieces)
    if len(pieces) == 2:
        return pieces[-1]
    else:
        return pieces[-2]


class JobQueuePlugin(SchedulerPlugin):
    def __init__(self):
        self.pending_jobs = OrderedDict()
        self.running_jobs = OrderedDict()
        self.finished_jobs = OrderedDict()

    def add_worker(self, scheduler, worker=None, name=None, **kwargs):
        ''' Run when a new worker enters the cluster'''
        w = scheduler.workers[worker]
        job_id = _job_id_from_worker_name(w.name)

        # if this is the first worker for this job, move job to running
        if job_id not in self.running_jobs:
            self.running_jobs[job_id] = self.pending_jobs.pop(job_id)

        # add worker to dict of workers in this job
        self.running_jobs[job_id][w.name] = w

    def remove_worker(self, scheduler=None, worker=None, **kwargs):
        ''' Run when a worker leaves the cluster'''
        job_id = _job_id_from_worker_name(worker)

        # remove worker from this job
        del self.running_jobs[job_id][worker]

        # once there are no more workers, move this job to finished_jobs
        if not self.running_jobs[job_id]:
            self.finished_jobs[job_id] = self.running_jobs.pop(job_id)


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
    scheduler_name = ''
    _adaptive_options = {
        'worker_key': lambda ws: _job_id_from_worker_name(ws.name)}

    def __init__(self,
                 name=None,
                 threads=None,
                 processes=None,
                 memory=None,
                 interface=None,
                 death_timeout=None,
                 local_directory=None,
                 extra=None,
                 env_extra=None,
                 walltime=None,
                 **kwargs
                 ):
        """ """
        # """
        # This initializer should be considered as Abstract, and never used
        # directly.
        # """
        if not self.scheduler_name:
            raise NotImplementedError('JobQueueCluster is an abstract class '
                                      'that should not be instanciated.')

        if name is None:
            name = dask.config.get('jobqueue.%s.name' % self.scheduler_name)
        if threads is None:
            threads = dask.config.get('jobqueue.%s.threads' % self.scheduler_name)
        if processes is None:
            processes = dask.config.get('jobqueue.%s.processes' % self.scheduler_name)
        if memory is None:
            memory = dask.config.get('jobqueue.%s.memory' % self.scheduler_name)
        if interface is None:
            interface = dask.config.get('jobqueue.%s.interface' % self.scheduler_name)
        if death_timeout is None:
            death_timeout = dask.config.get('jobqueue.%s.death-timeout' % self.scheduler_name)
        if local_directory is None:
            local_directory = dask.config.get('jobqueue.%s.local-directory' % self.scheduler_name)
        if extra is None:
            extra = dask.config.get('jobqueue.%s.extra' % self.scheduler_name)
        if env_extra is None:
            env_extra = dask.config.get('jobqueue.%s.env-extra' % self.scheduler_name)

        if '-' in name:
            raise ValueError(
                'name (%s) can not include the `-` character' % name)

        #This attribute should be overriden
        self.job_header = None

        if interface:
            host = get_ip_interface(interface)
            extra += ' --interface  %s ' % interface
        else:
            host = socket.gethostname()

        self.cluster = LocalCluster(n_workers=0, ip=host, **kwargs)

        # plugin for tracking job status
        self._scheduler_plugin = JobQueuePlugin()
        self.cluster.scheduler.add_plugin(self._scheduler_plugin)

        # Keep information on process, threads and memory, for use in
        # subclasses
        self.worker_memory = parse_bytes(memory) if memory is not None else None
        self.worker_processes = processes
        self.worker_threads = threads
        self.name = name

        self._adaptive = None

        self._env_header = '\n'.join(env_extra)

        # dask-worker command line build
        dask_worker_command = (
            '%(python)s -m distributed.cli.dask_worker' % dict(python=sys.executable))
        self._command_template = ' '.join([dask_worker_command, self.scheduler.address])
        if threads is not None:
            self._command_template += " --nthreads %d" % threads
        if processes is not None:
            self._command_template += " --nprocs %d" % processes
        if memory is not None:
            self._command_template += " --memory-limit %s" % memory
        if name is not None:
            # worker names follow this template: {NAME}-{JOB_ID}
            self._command_template += " --name %s-${JOB_ID}" % name
        if death_timeout is not None:
            self._command_template += " --death-timeout %s" % death_timeout
        if local_directory is not None:
            self._command_template += " --local-directory %s" % local_directory
        if extra is not None:
            self._command_template += extra

    @property
    def pending_jobs(self):
        """ Jobs pending in the queue """
        return self._scheduler_plugin.pending_jobs

    @property
    def running_jobs(self):
        """ Jobs with currenly active workers """
        return self._scheduler_plugin.running_jobs

    @property
    def finished_jobs(self):
        """ Jobs that have finished """
        return self._scheduler_plugin.finished_jobs

    def job_script(self):
        """ Construct a job submission script """
        pieces = {'job_header': self.job_header,
                  'env_header': self._env_header,
                  'worker_command': self._command_template}
        return self._script_template % pieces

    @contextmanager
    def job_file(self):
        """ Write job submission script to temporary file """
        with tmpfile(extension='sh') as fn:
            with open(fn, 'w') as f:
                f.write(self.job_script())
            yield fn

    def start_workers(self, n=1):
        """ Start workers and point them to our local scheduler """
        num_jobs = math.ceil(n / self.worker_processes)
        for _ in range(num_jobs):
            with self.job_file() as fn:
                out = self._call(shlex.split(self.submit_command) + [fn])
                job = self._job_id_from_submit_output(out.decode())
                self._scheduler_plugin.pending_jobs[job] = {}

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
        jobs = []
        for w in workers:
            if isinstance(w, dict):
                jobs.append(_job_id_from_worker_name(w['name']))
            else:
                jobs.append(_job_id_from_worker_name(w.name))
        self.stop_jobs(jobs)

    def stop_jobs(self, jobs):
        """ Stop a list of jobs"""
        if jobs:
            self._call([self.cancel_command] + list(jobs))

    def scale_up(self, n, **kwargs):
        """ Brings total worker count up to ``n`` """
        active_and_pending = sum([len(j.workers) for j in
                                  self.running_jobs.values()])
        active_and_pending += self.worker_processes * len(self.pending_jobs)
        self.start_workers(n - active_and_pending)

    def scale_down(self, workers):
        ''' Close the workers with the given addresses '''
        worker_states = []
        for w in workers:
            try:
                # Get the actual WorkerState
                worker_states.append(self.scheduler.workers[w])
            except KeyError:
                logger.debug('worker %s is already gone' % w)
        self.stop_workers(worker_states)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        jobs = list(self.pending_jobs.keys()) + list(self.running_jobs.keys())
        self.stop_jobs(jobs)
        self.cluster.__exit__(type, value, traceback)

    def _job_id_from_submit_output(self, out):
        raise NotImplementedError('_job_id_from_submit_output must be '
                                  'implemented when JobQueueCluster is '
                                  'inherited. It should convert the stdout '
                                  'from submit_command to the job id')
