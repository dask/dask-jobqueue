import functools

from .job import JobQueueCluster
from .pbs import PBSJob


class MoabJob(PBSJob):
    submit_command = "msub"
    cancel_command = "canceljob"
    scheduler_name = "moab"


MoabCluster = functools.partial(JobQueueCluster, Job=MoabJob, config_name="pbs")
