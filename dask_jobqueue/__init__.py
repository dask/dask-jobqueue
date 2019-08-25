# flake8: noqa
from . import config
from .core import JobQueueCluster
from .job import Job
from .moab import MoabCluster, MoabJob
from .pbs import PBSCluster, PBSJob
from .slurm import SLURMCluster, SLURMJob
from .sge import SGECluster, SGEJob
from .lsf import LSFCluster, LSFJob
from .oar import OARCluster
from .htcondor import HTCondorCluster

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
