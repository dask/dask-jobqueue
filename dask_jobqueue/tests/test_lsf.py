import os
from shutil import rmtree
import sys
from textwrap import dedent
import tempfile
from time import sleep, time

import dask
import pytest
from dask.distributed import Client
from distributed.utils import parse_bytes

from dask_jobqueue import LSFCluster, lsf

from . import QUEUE_WAIT


def test_header():
    with LSFCluster(walltime="00:02", processes=4, cores=8, memory="8GB") as cluster:

        assert "#BSUB" in cluster.job_header
        assert "#BSUB -J dask-worker" in cluster.job_header
        assert "#BSUB -n 8" in cluster.job_header
        assert "#BSUB -M 8000" in cluster.job_header
        assert "#BSUB -W 00:02" in cluster.job_header
        assert "#BSUB -q" not in cluster.job_header
        assert "#BSUB -P" not in cluster.job_header

    with LSFCluster(
        queue="general",
        project="DaskOnLSF",
        processes=4,
        cores=8,
        memory="28GB",
        ncpus=24,
        mem=100000000000,
    ) as cluster:

        assert "#BSUB -q general" in cluster.job_header
        assert "#BSUB -J dask-worker" in cluster.job_header
        assert "#BSUB -n 24" in cluster.job_header
        assert "#BSUB -n 8" not in cluster.job_header
        assert "#BSUB -M 100000" in cluster.job_header
        assert "#BSUB -M 28000" not in cluster.job_header
        assert "#BSUB -W" in cluster.job_header
        assert "#BSUB -P DaskOnLSF" in cluster.job_header

    with LSFCluster(cores=4, memory="8GB") as cluster:

        assert "#BSUB -n" in cluster.job_header
        assert "#BSUB -W" in cluster.job_header
        assert "#BSUB -M" in cluster.job_header
        assert "#BSUB -q" not in cluster.job_header
        assert "#BSUB -P" not in cluster.job_header

    with LSFCluster(
        cores=4, memory="8GB", job_extra=["-u email@domain.com"]
    ) as cluster:

        assert "#BSUB -u email@domain.com" in cluster.job_header
        assert "#BSUB -n" in cluster.job_header
        assert "#BSUB -W" in cluster.job_header
        assert "#BSUB -M" in cluster.job_header
        assert "#BSUB -q" not in cluster.job_header
        assert "#BSUB -P" not in cluster.job_header


def test_job_script():
    with LSFCluster(walltime="00:02", processes=4, cores=8, memory="28GB") as cluster:

        job_script = cluster.job_script()
        assert "#BSUB" in job_script
        assert "#BSUB -J dask-worker" in job_script
        assert "#BSUB -n 8" in job_script
        assert "#BSUB -M 28000" in job_script
        assert "#BSUB -W 00:02" in job_script
        assert "#BSUB -q" not in cluster.job_header
        assert "#BSUB -P" not in cluster.job_header

        assert (
            "{} -m distributed.cli.dask_worker tcp://".format(sys.executable)
            in job_script
        )
        assert "--nthreads 2 --nprocs 4 --memory-limit 7.00GB" in job_script

    with LSFCluster(
        queue="general",
        project="DaskOnLSF",
        processes=4,
        cores=8,
        memory="28GB",
        ncpus=24,
        mem=100000000000,
    ) as cluster:

        job_script = cluster.job_script()
        assert "#BSUB -q general" in cluster.job_header
        assert "#BSUB -J dask-worker" in cluster.job_header
        assert "#BSUB -n 24" in cluster.job_header
        assert "#BSUB -n 8" not in cluster.job_header
        assert "#BSUB -M 100000" in cluster.job_header
        assert "#BSUB -M 28000" not in cluster.job_header
        assert "#BSUB -W" in cluster.job_header
        assert "#BSUB -P DaskOnLSF" in cluster.job_header

        assert (
            "{} -m distributed.cli.dask_worker tcp://".format(sys.executable)
            in job_script
        )
        assert "--nthreads 2 --nprocs 4 --memory-limit 7.00GB" in job_script


# TODO common test
def test_config(loop):
    with dask.config.set(
        {
            "jobqueue.lsf.walltime": "00:02",
            "jobqueue.lsf.local-directory": "/foo",
            "jobqueue.lsf.use-stdin": True,
        }
    ):
        with LSFCluster(loop=loop, cores=1, memory="2GB") as cluster:
            assert "00:02" in cluster.job_script()
            assert "--local-directory /foo" in cluster.job_script()
            assert cluster._dummy_job.use_stdin


@pytest.mark.parametrize(
    "config_value,constructor_value",
    [
        (None, False),
        (None, True),
        (True, None),
        (False, None),
        (True, False),  # Constuctor overrides config
    ],
)
def test_use_stdin(loop, config_value, constructor_value):
    """
    Verify that use-stdin is respected when passed via the
    config OR the LSFCluster() constructor
    """
    with dask.config.set({"jobqueue.lsf.use-stdin": config_value}):
        with LSFCluster(
            loop=loop, cores=1, memory="2GB", use_stdin=constructor_value
        ) as cluster:
            if constructor_value is not None:
                assert cluster._dummy_job.use_stdin == constructor_value
            else:
                assert cluster._dummy_job.use_stdin == config_value


# TODO common test
def test_config_name_lsf_takes_custom_config():
    conf = {
        "queue": "myqueue",
        "project": "myproject",
        "ncpus": 1,
        "cores": 1,
        "mem": 2,
        "memory": "2 GB",
        "walltime": "00:02",
        "job-extra": [],
        "lsf-units": "TB",
        "name": "myname",
        "processes": 1,
        "interface": None,
        "death-timeout": None,
        "local-directory": "/foo",
        "extra": [],
        "env-extra": [],
        "log-directory": None,
        "shebang": "#!/usr/bin/env bash",
        "use-stdin": None,
    }

    with dask.config.set({"jobqueue.lsf-config-name": conf}):
        with LSFCluster(config_name="lsf-config-name") as cluster:
            assert cluster.job_name == "myname"

# TODO common test
def test_informative_errors():
    with pytest.raises(ValueError) as info:
        LSFCluster(memory=None, cores=4)
    assert "memory" in str(info.value)

    with pytest.raises(ValueError) as info:
        LSFCluster(memory="1GB", cores=None)
    assert "cores" in str(info.value)


def lsf_unit_detection_helper(expected_unit, conf_text=None):
    temp_dir = tempfile.mkdtemp()
    current_lsf_envdir = os.environ.get("LSF_ENVDIR", None)
    os.environ["LSF_ENVDIR"] = temp_dir
    if conf_text is not None:
        with open(os.path.join(temp_dir, "lsf.conf"), "w") as conf_file:
            conf_file.write(conf_text)
    memory_string = "13GB"
    memory_base = parse_bytes(memory_string)
    correct_memory = lsf.lsf_format_bytes_ceil(memory_base, lsf_units=expected_unit)
    with LSFCluster(memory=memory_string, cores=1) as cluster:
        assert "#BSUB -M %s" % correct_memory in cluster.job_header
    rmtree(temp_dir)
    if current_lsf_envdir is None:
        del os.environ["LSF_ENVDIR"]
    else:
        os.environ["LSF_ENVDIR"] = current_lsf_envdir


@pytest.mark.parametrize(
    "lsf_units_string,expected_unit",
    [
        ("LSF_UNIT_FOR_LIMITS=MB", "mb"),
        ("LSF_UNIT_FOR_LIMITS=G  # And a comment", "gb"),
        ("#LSF_UNIT_FOR_LIMITS=NotDetected", "kb"),
    ],
)
def test_lsf_unit_detection(lsf_units_string, expected_unit):
    conf_text = dedent(
        """
        LSB_JOB_MEMLIMIT=Y
        LSB_MOD_ALL_JOBS=N
        LSF_PIM_SLEEPTIME_UPDATE=Y
        LSF_PIM_LINUX_ENHANCE=Y
        %s
        LSB_DISABLE_LIMLOCK_EXCL=Y
        LSB_SUBK_SHOW_EXEC_HOST=Y
        """
        % lsf_units_string
    )
    lsf_unit_detection_helper(expected_unit, conf_text)


def test_lsf_unit_detection_without_file():
    lsf_unit_detection_helper("kb", conf_text=None)
