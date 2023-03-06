"""Namespace for dask_jobqueue.core.JobQueueCluster implementations."""
from typing import Dict

from .core import JobQueueCluster
from .htcondor import HTCondorJob, HTCondorCluster
from .local import LocalJob, LocalCluster
from .lsf import LSFJob, LSFCluster
from .moab import MoabJob, MoabCluster
from .oar import OARJob, OARCluster
from .pbs import PBSJob, PBSCluster
from .sge import SGEJob, SGECluster
from .slurm import SLURMJob, SLURMCluster

CLUSTER_CLASSES: Dict[str, JobQueueCluster] = {
    HTCondorJob.config_name: HTCondorCluster,
    LocalJob.config_name: LocalCluster,
    LSFJob.config_name: LSFCluster,
    MoabJob.config_name: MoabCluster,
    OARJob.config_name: OARCluster,
    PBSJob.config_name: PBSCluster,
    SGEJob.config_name: SGECluster,
    SLURMJob.config_name: SLURMCluster,
}
