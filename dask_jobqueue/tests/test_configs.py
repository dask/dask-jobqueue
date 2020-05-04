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

    scheduler_options = {
        "interface": config_scheduler_interface,
        "port": config_scheduler_port,
    }

    default_config_name = Cluster.job_cls.config_name

    with dask.config.set(
        {"jobqueue.%s.scheduler-options" % default_config_name: scheduler_options}
    ):

        with Cluster(cores=2, memory="2GB") as cluster:
            scheduler_options = cluster.scheduler_spec["options"]
            assert scheduler_options.get("interface") == config_scheduler_interface
            assert scheduler_options.get("port") == config_scheduler_port

        with Cluster(
            cores=2,
            memory="2GB",
            scheduler_options={"interface": pass_scheduler_interface},
        ) as cluster:
            scheduler_options = cluster.scheduler_spec["options"]
            assert scheduler_options.get("interface") == pass_scheduler_interface
            assert scheduler_options.get("port") is None
