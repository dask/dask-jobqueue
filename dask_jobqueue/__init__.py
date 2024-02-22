# flake8: noqa
from . import config
from .core import JobQueueCluster
from .moab import MoabCluster
from .pbs import PBSCluster
from .slurm import SLURMCluster
from .sge import SGECluster
from .lsf import LSFCluster
from .oar import OARCluster
from .htcondor import HTCondorCluster

from . import _version

__version__ = _version.get_versions()["version"]
