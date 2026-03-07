import sys
import warnings

import dask
import pytest
from dask.utils import format_bytes, parse_bytes

from dask_jobqueue import FluxCluster
from dask_jobqueue.flux import FluxJob, _flux_walltime, _without_arg


def test_without_arg_removes_only_requested_option():
    command = (
        "python -m distributed.cli.dask_worker tcp://127.0.0.1:8786 "
        "--name worker-0 --nthreads 2 --nworkers 4 --memory-limit 1GiB"
    )

    assert _without_arg(command, "--nworkers") == (
        "python -m distributed.cli.dask_worker tcp://127.0.0.1:8786 "
        "--name worker-0 --nthreads 2 --memory-limit 1GiB"
    )
    assert "--nworkers 4" in _without_arg(command, "--no-such-option")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, None),
        ("45m", "45m"),
        ("00:10:00", "10m"),
        ("01:00:00", "1h"),
        ("00:00:59", "59s"),
        ("01:01:01", "3661s"),
    ],
)
def test_flux_walltime_normalization(raw, expected):
    assert _flux_walltime(raw) == expected


def test_flux_job_rejects_invalid_job_nodes():
    with pytest.raises(ValueError, match="job_nodes must be at least 1"):
        FluxJob(
            scheduler="tcp://127.0.0.1:8786",
            cores=2,
            memory="4GB",
            processes=1,
            job_nodes=0,
        )


def test_header():
    with FluxCluster(
        walltime="45m",
        processes=4,
        cores=8,
        memory="28GB",
        name="dask-worker",
    ) as cluster:
        assert "#flux: --job-name=dask-worker" in cluster.job_header
        assert "#flux: -N 1" in cluster.job_header
        assert "#flux: -n 1" in cluster.job_header
        assert "#flux: -c 8" in cluster.job_header
        assert "#flux: -t 45m" in cluster.job_header
        assert "#flux: -q" not in cluster.job_header
        assert "#flux: -B" not in cluster.job_header

    with FluxCluster(
        queue="debug",
        account="project123",
        walltime="30m",
        processes=2,
        cores=4,
        memory="8GB",
        job_cpu=6,
        job_nodes=3,
        job_extra_directives=["--exclusive"],
    ) as cluster:
        assert "#flux: -q debug" in cluster.job_header
        assert "#flux: -B project123" in cluster.job_header
        assert "#flux: -N 3" in cluster.job_header
        assert "#flux: -n 2" in cluster.job_header
        assert "#flux: -c 6" in cluster.job_header
        assert "#flux: --exclusive" in cluster.job_header


def test_job_script():
    with FluxCluster(
        walltime="45m",
        processes=4,
        cores=8,
        memory="28GB",
    ) as cluster:
        job_script = cluster.job_script()
        formatted_bytes = format_bytes(parse_bytes("7GB")).replace(" ", "")

        assert "#flux: --job-name=dask-worker" in job_script
        assert "#flux: -N 1" in job_script
        assert "#flux: -n 1" in job_script
        assert "#flux: -c 8" in job_script
        assert "#flux: -t 45m" in job_script
        assert "#flux: -q" not in job_script
        assert "#flux: -B" not in job_script
        assert f"{sys.executable} -m distributed.cli.dask_worker tcp://" in job_script
        assert "--nthreads 2" in job_script
        assert "--nworkers 4" in job_script
        assert f"--memory-limit {formatted_bytes}" in job_script
        assert "flux run" not in job_script

    with FluxCluster(
        queue="debug",
        account="project123",
        walltime="30m",
        processes=2,
        cores=4,
        memory="8GB",
        job_nodes=3,
        job_script_prologue=['echo "starting"'],
        job_script_epilogue=['echo "done"'],
    ) as cluster:
        job_script = cluster.job_script()
        formatted_bytes = format_bytes(parse_bytes("4GB")).replace(" ", "")

        assert "#flux: -q debug" in job_script
        assert "#flux: -B project123" in job_script
        assert "#flux: -N 3" in job_script
        assert "#flux: -n 2" in job_script
        assert "#flux: -c 2" in job_script
        assert "flux run -N 3 -n 2" in job_script
        assert "--nworkers" not in job_script
        assert "--nthreads 2" in job_script
        assert f"--memory-limit {formatted_bytes}" in job_script
        assert 'echo "starting"' in job_script
        assert 'echo "done"' in job_script


def test_job_script_normalizes_hms_walltime():
    with FluxCluster(
        queue="pdebug",
        walltime="00:10:00",
        processes=1,
        cores=2,
        memory="4GB",
    ) as cluster:
        job_script = cluster.job_script()

        assert "#flux: -t 10m" in job_script
        assert "#flux: -t 00:10:00" not in job_script


def test_header_lines_skip():
    job = FluxJob(cores=1, memory="1GB", job_name="foobar")
    assert "foobar" in job.job_script()

    job = FluxJob(cores=1, memory="1GB", job_name="foobar", job_directives_skip=["--job-name"])
    assert "foobar" not in job.job_script()



def test_header_lines_dont_skip_extra_directives():
    job = FluxJob(
        cores=1, memory="1GB", job_name="foobar", job_extra_directives=["--job-name=custom"]
    )
    assert "foobar" in job.job_script()
    assert "--job-name=custom" in job.job_script()

    job = FluxJob(
        cores=1,
        memory="1GB",
        job_name="foobar",
        job_directives_skip=["--job-name"],
        job_extra_directives=["--job-name=custom"],
    )
    assert "foobar" not in job.job_script()
    assert "--job-name=custom" in job.job_script()



def test_deprecation_header_skip():
    warnings.simplefilter("ignore", UserWarning)

    with warnings.catch_warnings(record=True) as caught:
        FluxJob(cores=1, memory="1 GB", header_skip=["old_param"])
        assert len(caught) == 1
        assert issubclass(caught[0].category, FutureWarning)
        assert "header_skip has been renamed" in str(caught[0].message)

    with warnings.catch_warnings(record=True) as caught:
        FluxJob(
            cores=1,
            memory="1 GB",
            header_skip=["old_param"],
            job_directives_skip=["new_param"],
        )
        assert len(caught) == 1
        assert issubclass(caught[0].category, FutureWarning)
        assert "header_skip has been renamed" in str(caught[0].message)

    with warnings.catch_warnings(record=True) as caught:
        FluxJob(
            cores=1,
            memory="1 GB",
            job_directives_skip=["new_param"],
        )
        assert len(caught) == 0

    warnings.simplefilter("ignore")
    job = FluxJob(
        cores=1,
        memory="1 GB",
        job_name="jobname",
        header_skip=["jobname"],
        job_directives_skip=["new_param"],
    )
    assert "jobname" in job.job_script()

    job = FluxJob(
        cores=1,
        memory="1 GB",
        job_name="jobname",
        header_skip=["jobname"],
    )
    assert "jobname" not in job.job_script()

    job = FluxJob(
        cores=1,
        memory="1 GB",
        job_name="jobname",
        header_skip=["jobname"],
        job_directives_skip=(),
    )
    assert "jobname" not in job.job_script()



def test_config_name_flux_takes_custom_config():
    conf = {
        "name": "myname",
        "cores": 1,
        "memory": "2 GB",
        "processes": 1,
        "python": None,
        "interface": None,
        "death-timeout": None,
        "local-directory": "/foo",
        "shared-temp-directory": None,
        "extra": None,
        "worker-command": None,
        "worker-extra-args": [],
        "queue": "myqueue",
        "account": "myaccount",
        "walltime": "00:02:00",
        "env-extra": None,
        "job-script-prologue": [],
        "job-script-epilogue": [],
        "job-extra": None,
        "job-extra-directives": [],
        "job-directives-skip": [],
        "log-directory": None,
        "shebang": "#!/usr/bin/env bash",
        "job-cpu": None,
        "job-nodes": 1,
    }

    with dask.config.set({"jobqueue.flux-config-name": conf}):
        with FluxCluster(config_name="flux-config-name") as cluster:
            assert cluster.job_name == "myname"
            assert "#flux: -q myqueue" in cluster.job_header
            assert "#flux: -B myaccount" in cluster.job_header
            assert "#flux: -t 2m" in cluster.job_header
