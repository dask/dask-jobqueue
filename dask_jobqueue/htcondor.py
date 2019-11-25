import logging
import re
import shlex

import dask
from distributed.utils import parse_bytes

from .core import JobQueueCluster, Job, job_parameters, cluster_parameters

logger = logging.getLogger(__name__)


class HTCondorJob(Job):
    submit_command = "condor_submit"
    cancel_command = "condor_rm"
    job_id_regexp = r"(?P<job_id>\d+\.\d+)"

    # condor sets argv[0] of the executable to "condor_exec.exe", which confuses
    # Python (can't find its libs), so we have to go through the shell.
    executable = "/bin/sh"

    config_name = "htcondor"

    def __init__(
        self,
        scheduler=None,
        name=None,
        disk=None,
        job_extra=None,
        config_name=None,
        **base_class_kwargs
    ):
        super().__init__(
            scheduler=scheduler, name=name, config_name=config_name, **base_class_kwargs
        )

        if disk is None:
            disk = dask.config.get("jobqueue.%s.disk" % self.config_name)
        if disk is None:
            raise ValueError(
                "You must specify how much disk to use per job like ``disk='1 GB'``"
            )
        self.worker_disk = parse_bytes(disk)
        if job_extra is None:
            self.job_extra = dask.config.get(
                "jobqueue.%s.job-extra" % self.config_name, {}
            )
        else:
            self.job_extra = job_extra

        env_extra = base_class_kwargs.get("env_extra", None)
        if env_extra is None:
            env_extra = dask.config.get(
                "jobqueue.%s.env-extra" % self.config_name, default=[]
            )
        self.env_extra = env_extra

    def job_script(self):
        """ Construct a job submission script """
        return self.template_env.get_template("htcondor_script.sh").render(
            shebang=self.shebang,
            worker_command=self._command_template,
            executable=self.executable,
            log_directory=self.log_directory,
            worker_cores=self.worker_cores,
            worker_disk=self.worker_disk,
            worker_memory=self.worker_memory,
            job_extra=self.job_extra,
            env_extra=self.env_extra,
        )

    def _job_id_from_submit_output(self, out):
        cluster_id_regexp = r"submitted to cluster (\d+)"
        match = re.search(cluster_id_regexp, out)
        if match is None:
            msg = (
                "Could not parse cluster id from submission command output.\n"
                "Cluster id regexp is {!r}\n"
                "Submission command output is:\n{}".format(cluster_id_regexp, out)
            )
            raise ValueError(msg)
        return "%s.0" % match.group(1)

    @property
    def template_env(self):
        env = super().template_env
        env.filters['env_lines_to_dict'] = env_lines_to_dict
        env.filters['quote_environment'] = quote_environment
        return env


def env_lines_to_dict(env_lines):
    """ Convert an array of export statements (what we get from env-extra
    in the config) into a dict """
    env_dict = {}
    for env_line in env_lines:
        split_env_line = shlex.split(env_line)
        if split_env_line[0] == "export":
            split_env_line = split_env_line[1:]
        for item in split_env_line:
            if "=" in item:
                k, v = item.split("=", 1)
                env_dict[k] = v
    return env_dict


def _double_up_quotes(instr):
    return instr.replace("'", "''").replace('"', '""')


def quote_environment(env):
    """Quote a dict of strings using the Condor submit file "new" environment quoting rules.

    Returns
    -------
    str
        The environment in quoted form.

    Warnings
    --------
    You will need to surround the result in double-quotes before using it in
    the Environment attribute.

    Examples
    --------
    >>> from collections import OrderedDict
    >>> quote_environment(OrderedDict([("one", 1), ("two", '"2"'), ("three", "spacey 'quoted' value")]))
    'one=1 two=""2"" three=\'spacey \'\'quoted\'\' value\''
    """
    if not isinstance(env, dict):
        raise TypeError("env must be a dict")

    entries = []
    for k, v in env.items():
        qv = _double_up_quotes(str(v))
        if " " in qv or "'" in qv:
            qv = "'" + qv + "'"
        entries.append("%s=%s" % (k, qv))

    return " ".join(entries)


class HTCondorCluster(JobQueueCluster):
    __doc__ = """ Launch Dask on an HTCondor cluster with a shared file system

    Parameters
    ----------
    disk : str
        Total amount of disk per job
    job_extra : dict
        Extra submit file attributes for the job
    {job}
    {cluster}

    Examples
    --------
    >>> from dask_jobqueue.htcondor import HTCondorCluster
    >>> cluster = HTCondorCluster(cores=24, memory="4GB", disk="4GB")
    >>> cluster.scale(jobs=10)  # ask for 10 jobs

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt(maximum_jobs=20)
    """.format(
        job=job_parameters, cluster=cluster_parameters
    )
    job_cls = HTCondorJob
