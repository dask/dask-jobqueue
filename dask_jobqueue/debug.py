from __future__ import absolute_import, division, print_function

import logging
import os

import dask

from .core import JobQueueCluster, docstrings

logger = logging.getLogger(__name__)


class DEBUGCluster(JobQueueCluster):
    __doc__ = docstrings.with_indents(""" Launch Dask locally with os.system calls

    Parameters
    ----------
    %(JobQueueCluster.parameters)s

    Examples
    --------
    >>> from dask_jobqueue import DEBUGCluster
    >>> cluster = DEBUGCluster(queue='regular')
    >>> cluster.scale(10)  # this may take a few seconds to launch

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt()
    """, 4)

    # Override class variables
    submit_command = 'python '
    cancel_command = 'kill '

    def __init__(self, config_name='debug', **kwargs):
        super(DEBUGCluster, self).__init__(config_name=config_name, **kwargs)
        if "python" not in self.shebang:
            self.shebang = "#!/usr/bin/env python"
        og_cmd_template = self._command_template
        self._command_template = ("import subprocess\n"
                                  "import shlex\n"
                                  "CMD='xxx'\n"
                                  "lf = open('logfile', 'a')\n"
                                  "print(subprocess.Popen(shlex.split(CMD), stderr=subprocess.STDOUT, stdout=lf).pid)")
                                  #"print(subprocess.Popen(shlex.split(CMD), stderr=subprocess.STDOUT, stdout=subprocess.DEVNULL).pid)"
        self._command_template = self._command_template.replace('xxx', og_cmd_template)
        self._command_template = self._command_template.replace("${JOB_ID}", "pid")

        self.job_header = ''
