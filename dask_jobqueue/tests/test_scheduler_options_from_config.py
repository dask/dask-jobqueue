import pytest, psutil, dask

from dask_jobqueue import (
    JobQueueCluster,
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
    config_scheduler_host = net_if_addrs[config_scheduler_interface][0].address
    config_scheduler_port = 8804

    pass_scheduler_interface = list(net_if_addrs.keys())[1]
    pass_scheduler_host = net_if_addrs[pass_scheduler_interface][0].address
    pass_scheduler_port = 8807

    generic_cluster_conf = {
        # configurable entries collected from
        # all the available clusters
        # they are not actually useful here,
        # but are necessary for this to work...
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

    with dask.config.set({"jobqueue.generic-config": generic_cluster_conf}):
        with Cluster(cores=2, memory="2GB", config_name="generic-config") as cluster:
            assert cluster.scheduler_spec["options"].get("interface") == None
            assert cluster.scheduler_spec["options"].get("port") == None

    generic_cluster_conf["scheduler_options"] = {
        "interface": config_scheduler_interface,
        "port": config_scheduler_port,
    }

    with dask.config.set({"jobqueue.generic-config": generic_cluster_conf}):

        with Cluster(cores=2, memory="2GB", config_name="generic-config",) as cluster:
            assert (
                cluster.scheduler_spec["options"].get("interface")
                == config_scheduler_interface
            )
            assert (
                cluster.scheduler_spec["options"].get("port") == config_scheduler_port
            )

        with Cluster(
            cores=2,
            memory="2GB",
            config_name="generic-config",
            scheduler_options={
                "interface": pass_scheduler_interface,
                "port": pass_scheduler_port,
            },
        ) as cluster:
            assert (
                cluster.scheduler_spec["options"].get("interface")
                == pass_scheduler_interface
            )
            assert cluster.scheduler_spec["options"].get("port") == pass_scheduler_port

    generic_cluster_conf["scheduler_options"] = {"host": config_scheduler_host}

    with dask.config.set({"jobqueue.generic-config": generic_cluster_conf}):

        with Cluster(cores=2, memory="2GB", config_name="generic-config",) as cluster:
            assert (
                cluster.scheduler_spec["options"].get("host") == config_scheduler_host
            )

        with Cluster(
            cores=2,
            memory="2GB",
            config_name="generic-config",
            scheduler_options={"host": pass_scheduler_host,},
        ) as cluster:
            assert cluster.scheduler_spec["options"].get("host") == pass_scheduler_host
