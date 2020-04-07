import pytest
import psutil
import dask

from dask_jobqueue import (
    PBSCluster,
    MoabCluster,
    SLURMCluster,
    SGECluster,
    LSFCluster,
    OARCluster,
)


@pytest.mark.parametrize(
    "Cluster",
    [PBSCluster, MoabCluster, SLURMCluster, SGECluster, LSFCluster, OARCluster],
)
def test_import_scheduler_options_from_config(Cluster):

    net_if_addrs = psutil.net_if_addrs()

    config_scheduler_interface = list(net_if_addrs.keys())[0]
    config_scheduler_port = 8804

    pass_scheduler_interface = list(net_if_addrs.keys())[1]

    generic_cluster_conf = {
        "cores": None,
        "memory": None,
        "queue": None,
        "project": None,
        "ncpus": None,
        "processes": None,
        "walltime": None,
        "job-extra": [],
        "name": None,
        "interface": None,
        "death-timeout": None,
        "local-directory": None,
        "extra": [],
        "env-extra": [],
        "log-directory": None,
        "shebang": None,
        "job-cpu": None,
        "job-mem": None,
        "resource-spec": None,
        "mem": None,
        "lsf-units": None,
        "use-stdin": None,
    }

    generic_cluster_conf["scheduler_options"] = {
        "interface": config_scheduler_interface,
        "port": config_scheduler_port,
    }

    with dask.config.set({"jobqueue.generic-config": generic_cluster_conf}):

        with Cluster(cores=2, memory="2GB", config_name="generic-config",) as cluster:
            scheduler_options = cluster.scheduler_spec["options"]
            assert scheduler_options.get("interface") == config_scheduler_interface
            assert scheduler_options.get("port") == config_scheduler_port

        with Cluster(
            cores=2,
            memory="2GB",
            config_name="generic-config",
            scheduler_options={"interface": pass_scheduler_interface},
        ) as cluster:
            scheduler_options = cluster.scheduler_spec["options"]
            assert scheduler_options.get("interface") == pass_scheduler_interface
            assert scheduler_options.get("port") is None
