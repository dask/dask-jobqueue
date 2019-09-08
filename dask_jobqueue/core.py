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
    %{job_parameters}s

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
