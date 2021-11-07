# content of conftest.py

# Make loop fixture available in all tests
from distributed.utils_test import loop  # noqa: F401

import pytest

import dask_jobqueue.lsf
import dask

from dask_jobqueue import (
    PBSCluster,
    MoabCluster,
    SLURMCluster,
    SGECluster,
    LSFCluster,
    OARCluster,
    HTCondorCluster,
)

from dask_jobqueue.local import LocalCluster


def pytest_addoption(parser):
    parser.addoption(
        "-E",
        action="store",
        metavar="NAME",
        help="only run tests matching the environment NAME.",
    )


def pytest_configure(config):
    # register an additional marker
    config.addinivalue_line(
        "markers", "env(NAME): only run test if environment NAME matches"
    )
    config.addinivalue_line("markers", "xfail_ci(NAME): know CI failure")


def pytest_runtest_setup(item):
    env = item.config.getoption("-E")
    envnames = sum(
        [
            mark.args[0] if isinstance(mark.args[0], list) else [mark.args[0]]
            for mark in item.iter_markers(name="env")
        ],
        [],
    )
    if (
        None not in envnames
        and (env is None and envnames)
        or (env is not None and env not in envnames)
    ):
        pytest.skip("test requires env in %r" % envnames)
    else:
        xfail = {}
        [xfail.update(mark.args[0]) for mark in item.iter_markers(name="xfail_ci")]
        if env in xfail:
            item.add_marker(pytest.mark.xfail(reason=xfail[env]))


@pytest.fixture(autouse=True)
def mock_lsf_version(monkeypatch, request):
    # Monkey-patch lsf_version() UNLESS the 'lsf' environment is selected.
    # In that case, the real lsf_version() function should work.
    markers = list(request.node.iter_markers())
    if any("lsf" in marker.args for marker in markers):
        return

    try:
        dask_jobqueue.lsf.lsf_version()
    except OSError:
        # Provide a fake implementation of lsf_version()
        monkeypatch.setattr(dask_jobqueue.lsf, "lsf_version", lambda: "10")


all_envs = {
    None: LocalCluster,
    "pbs": PBSCluster,
    "moab": MoabCluster,
    "slurm": SLURMCluster,
    "sge": SGECluster,
    "lsf": LSFCluster,
    "oar": OARCluster,
    "htcondor": HTCondorCluster,
}


@pytest.fixture
def EnvSpecificCluster3(pytestconfig):
    return all_envs[pytestconfig.getoption("-E")]


@pytest.fixture(
    params=[pytest.param(v, marks=[pytest.mark.env(k)]) for (k, v) in all_envs.items()]
)
def EnvSpecificCluster(request):
    if request.param == HTCondorCluster:
        dask.config.set({"jobqueue.htcondor.disk": "20MB"})
    return request.param


@pytest.fixture(params=list(all_envs.values()))
def Cluster(request):
    if request.param == HTCondorCluster:
        dask.config.set({"jobqueue.htcondor.disk": "1GB"})
    return request.param
