import docrep

docstrings = docrep.DocstringProcessor()


# TODO: remove this class after we figure out docstrings


@docstrings.get_sectionsf("JobQueueCluster")
class JobQueueCluster:
    """ Base class to launch Dask Clusters for Job queues

    This class should not be used directly, use inherited class appropriate for your queueing system (e.g. PBScluster
    or SLURMCluster)

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
    log_directory : str
        Directory to use for job scheduler logs.
    shebang : str
        Path to desired interpreter for your batch submission script.
    python : str
        Python executable used to launch Dask workers.
    config_name : str
        Section to use from jobqueue.yaml configuration file.
    kwargs : dict
        Additional keyword arguments to pass to `LocalCluster`

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
