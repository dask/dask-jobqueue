from contextlib import contextmanager
import logging
import os
import re
import shlex
import subprocess
import sys
import weakref

import dask
from distributed.deploy.spec import ProcessInterface, SpecCluster
from distributed.scheduler import Scheduler

from distributed.utils import format_bytes, parse_bytes, tmpfile, get_ip_interface

logger = logging.getLogger(__name__)


class Job(ProcessInterface):
    """ Base class to launch Dask workers on Job queues

    This class should not be used directly, use inherited class appropriate for
    your queueing system (e.g. PBScluster or SLURMCluster)

    Parameters
    ----------
    name : str
        Name of Dask workers.
    cores : int
        Total number of cores per job
    memory: str
        Total amount of memory per job
    processes : int
        Number of processes per job
    nanny : bool
        Whether or not to start a nanny process
    interface : str
        Network interface like 'eth0' or 'ib0'.
    death_timeout : float
        Seconds to wait for a scheduler before closing workers
    local_directory : str
        Dask worker local directory for file spilling.
    extra : list
        Additional arguments to pass to `dask-worker`
    env_extra : list
        Other commands to add to script before launching worker.
    header_skip : list
        Lines to skip in the header.
        Header lines matching this text will be removed
    log_directory : str
        Directory to use for job scheduler logs.
    shebang : str
        Path to desired interpreter for your batch submission script.
    python : str
        Python executable used to launch Dask workers.
    config_name : str
        Section to use from jobqueue.yaml configuration file.

    Attributes
    ----------
    submit_command: str
        Abstract attribute for job scheduler submit command,
        should be overridden
    cancel_command: str
        Abstract attribute for job scheduler cancel command,
        should be overridden

    See Also
    --------
    PBSCluster
    SLURMCluster
    SGECluster
    OARCluster
    LSFCluster
    MoabCluster
    """

    _script_template = """
%(shebang)s

%(job_header)s
%(env_header)s
%(worker_command)s
""".lstrip()

    # Following class attributes should be overridden by extending classes.
    submit_command = None
    cancel_command = None
    job_id_regexp = r"(?P<job_id>\d+)"

    def __init__(
        self,
        scheduler=None,
        name=None,
        cores=None,
        memory=None,
        processes=None,
        nanny=True,
        interface=None,
        death_timeout=None,
        local_directory=None,
        extra=None,
        env_extra=None,
        header_skip=None,
        log_directory=None,
        shebang=None,
        python=sys.executable,
        job_name=None,
        config_name=None,
        **kwargs
    ):
        # """
        # This initializer should be considered as Abstract, and never used directly.
        # """
        self.scheduler = scheduler
        self.job_id = None

        super().__init__()
        if config_name is None:
            config_name = getattr(type(self), "config_name", None)

        if config_name is None:
            raise NotImplementedError(
                "JobQueueCluster is an abstract class that should not be instantiated."
            )

        if job_name is None:
            job_name = dask.config.get("jobqueue.%s.name" % config_name)
        if cores is None:
            cores = dask.config.get("jobqueue.%s.cores" % config_name)
        if memory is None:
            memory = dask.config.get("jobqueue.%s.memory" % config_name)
        if processes is None:
            processes = dask.config.get("jobqueue.%s.processes" % config_name)
        if interface is None:
            interface = dask.config.get("jobqueue.%s.interface" % config_name)
        if death_timeout is None:
            death_timeout = dask.config.get("jobqueue.%s.death-timeout" % config_name)
        if local_directory is None:
            local_directory = dask.config.get(
                "jobqueue.%s.local-directory" % config_name
            )
        if extra is None:
            extra = dask.config.get("jobqueue.%s.extra" % config_name)
        if env_extra is None:
            env_extra = dask.config.get("jobqueue.%s.env-extra" % config_name)
        if header_skip is None:
            header_skip = dask.config.get("jobqueue.%s.header-skip" % config_name, ())
        if log_directory is None:
            log_directory = dask.config.get("jobqueue.%s.log-directory" % config_name)
        if shebang is None:
            shebang = dask.config.get("jobqueue.%s.shebang" % config_name)

        if cores is None or memory is None:
            raise ValueError(
                "You must specify how much cores and memory per job you want to use, for example:\n"
                "cluster = {}(cores={}, memory={!r})".format(
                    self.__class__.__name__, cores or 8, memory or "24GB"
                )
            )

        # This attribute should be overridden
        self.job_header = None

        if interface:
            extra += ["--interface", interface]
            kwargs.setdefault("host", get_ip_interface(interface))
        else:
            kwargs.setdefault("host", "")

        # Keep information on process, cores, and memory, for use in subclasses
        self.worker_memory = parse_bytes(memory) if memory is not None else None
        self.worker_processes = processes
        self.worker_cores = cores
        self.name = name
        self.job_name = job_name

        self.shebang = shebang

        self._env_header = "\n".join(filter(None, env_extra))
        self.header_skip = set(header_skip)

        # dask-worker command line build
        dask_worker_command = "%(python)s -m distributed.cli.dask_worker" % dict(
            python=python
        )
        command_args = [dask_worker_command, self.scheduler]
        command_args += ["--nthreads", self.worker_process_threads]
        if processes is not None and processes > 1:
            command_args += ["--nprocs", processes]

        command_args += ["--memory-limit", self.worker_process_memory]
        command_args += ["--name", str(name)]
        command_args += ["--nanny" if nanny else "--no-nanny"]

        if death_timeout is not None:
            command_args += ["--death-timeout", death_timeout]
        if local_directory is not None:
            command_args += ["--local-directory", local_directory]
        if extra is not None:
            command_args += extra

        self._command_template = " ".join(map(str, command_args))

        self.log_directory = log_directory
        if self.log_directory is not None:
            if not os.path.exists(self.log_directory):
                os.makedirs(self.log_directory)

    def job_script(self):
        """ Construct a job submission script """
        header = "\n".join(
            [
                line
                for line in self.job_header.split("\n")
                if not any(skip in line for skip in self.header_skip)
            ]
        )
        pieces = {
            "shebang": self.shebang,
            "job_header": header,
            "env_header": self._env_header,
            "worker_command": self._command_template,
        }
        return self._script_template % pieces

    @contextmanager
    def job_file(self):
        """ Write job submission script to temporary file """
        with tmpfile(extension="sh") as fn:
            with open(fn, "w") as f:
                logger.debug("writing job script: \n%s", self.job_script())
                f.write(self.job_script())
            yield fn

    def _submit_job(self, script_filename):
        # Should we make this async friendly?
        return self._call(shlex.split(self.submit_command) + [script_filename])

    @property
    def worker_process_threads(self):
        return int(self.worker_cores / self.worker_processes)

    @property
    def worker_process_memory(self):
        mem = format_bytes(self.worker_memory / self.worker_processes)
        mem = mem.replace(" ", "")
        return mem

    async def start(self):
        """ Start workers and point them to our local scheduler """
        logger.debug("Starting worker: %s", self.name)

        with self.job_file() as fn:
            out = self._submit_job(fn)
            job = self._job_id_from_submit_output(out)
            if not job:
                raise ValueError("Unable to parse jobid from output of %s" % out)
            self.job_id = job

        weakref.finalize(self, self._close_job, job)

        logger.debug("Starting job: %s", self.job_id)
        await super().start()

    def _job_id_from_submit_output(self, out):
        match = re.search(self.job_id_regexp, out)
        if match is None:
            msg = (
                "Could not parse job id from submission command "
                "output.\nJob id regexp is {!r}\nSubmission command "
                "output is:\n{}".format(self.job_id_regexp, out)
            )
            raise ValueError(msg)

        job_id = match.groupdict().get("job_id")
        if job_id is None:
            msg = (
                "You need to use a 'job_id' named group in your regexp, e.g. "
                "r'(?P<job_id>\\d+)', in your regexp. Your regexp was: "
                "{!r}".format(self.job_id_regexp)
            )
            raise ValueError(msg)

        return job_id

    async def close(self):
        logger.debug("Stopping worker: %s job: %s", self.name, self.job_id)
        self._close_job(self.job_id)

    @classmethod
    def _close_job(cls, job_id):
        if job_id:
            cls._call(shlex.split(cls.cancel_command) + [job_id])

    @staticmethod
    def _call(cmd, **kwargs):
        """ Call a command using subprocess.Popen.

        This centralizes calls out to the command line, providing consistent
        outputs, logging, and an opportunity to go asynchronous in the future.

        Parameters
        ----------
        cmd: List(str))
            A command, each of which is a list of strings to hand to
            subprocess.Popen

        Examples
        --------
        >>> self._call(['ls', '/foo'])

        Returns
        -------
        The stdout produced by the command, as string.

        Raises
        ------
        RuntimeError if the command exits with a non-zero exit code
        """
        cmd_str = " ".join(cmd)
        logger.debug(
            "Executing the following command to command line\n{}".format(cmd_str)
        )

        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs
        )

        out, err = proc.communicate()
        out, err = out.decode(), err.decode()

        if proc.returncode != 0:
            raise RuntimeError(
                "Command exited with non-zero exit code.\n"
                "Exit code: {}\n"
                "Command:\n{}\n"
                "stdout:\n{}\n"
                "stderr:\n{}\n".format(proc.returncode, cmd_str, out, err)
            )
        return out


class JobQueueCluster(SpecCluster):
    def __init__(
        self,
        n_workers=0,
        Job: Job = None,
        # Cluster keywords
        loop=None,
        security=None,
        silence_logs="error",
        name=None,
        asynchronous=False,
        # Scheduler keywords
        interface=None,
        host=None,
        protocol="tcp://",
        dashboard_address=":8787",
        config_name=None,
        # Job keywords
        **kwargs
    ):
        self.status = "created"
        if Job is None:
            raise ValueError(
                "You must provide a Job type like PBSJob, SLURMJob, "
                "or SGEJob with the Job= argument."
            )

        if config_name:
            if interface is None:
                interface = dask.config.get("jobqueue.%s.interface" % config_name)

        scheduler = {
            "cls": Scheduler,  # Use local scheduler for now
            "options": {
                "protocol": protocol,
                "interface": interface,
                "host": host,
                "dashboard_address": dashboard_address,
                "security": security,
            },
        }
        if config_name:
            kwargs["config_name"] = config_name
        kwargs["interface"] = interface
        kwargs["protocol"] = protocol
        kwargs["security"] = security
        self._kwargs = kwargs
        self._Job = Job
        worker = {"cls": Job, "options": kwargs}
        if "processes" in kwargs and kwargs["processes"] > 1:
            worker["group"] = ["-" + str(i) for i in range(kwargs["processes"])]

        self.example_job  # trigger property to ensure that the job is valid

        super().__init__(
            scheduler=scheduler,
            worker=worker,
            loop=loop,
            silence_logs=silence_logs,
            asynchronous=asynchronous,
            name=name,
        )

        if n_workers:
            self.scale(n_workers)

    @property
    def example_job(self):
        try:
            address = self.scheduler.address
        except AttributeError:
            address = "tcp://scheduler:8786"
        return self._Job(address or "tcp://scheduler:8786", name="name", **self._kwargs)

    @property
    def job_header(self):
        return self.example_job.job_header

    def job_script(self):
        return self.example_job.job_script()

    @property
    def job_name(self):
        return self.example_job.job_name


class EmptyJob(ProcessInterface):
    pass
