import os
import shutil
import socket
import sys
import re
import psutil

import pytest

import dask

from distributed.security import Security
from distributed import Client

from dask_jobqueue import (
    JobQueueCluster,
    HTCondorCluster,
)
from dask_jobqueue.core import Job
from dask_jobqueue.local import LocalCluster

from dask_jobqueue.sge import SGEJob


def test_errors():
    match = re.compile("Job type.*job_cls", flags=re.DOTALL)
    with pytest.raises(ValueError, match=match):
        JobQueueCluster(cores=4)


def test_command_template(Cluster):
    with Cluster(cores=2, memory="4GB") as cluster:
        assert (
            "%s -m distributed.cli.dask_worker" % (sys.executable)
            in cluster._dummy_job._command_template
        )
        assert " --nthreads 1" in cluster._dummy_job._command_template
        assert " --memory-limit " in cluster._dummy_job._command_template
        assert " --name " in cluster._dummy_job._command_template

    with Cluster(
        cores=2,
        memory="4GB",
        death_timeout=60,
        local_directory="/scratch",
        worker_extra_args=["--preload", "mymodule"],
    ) as cluster:
        assert " --death-timeout 60" in cluster._dummy_job._command_template
        assert " --local-directory /scratch" in cluster._dummy_job._command_template
        assert " --preload mymodule" in cluster._dummy_job._command_template


def test_shebang_settings(Cluster, request):
    if Cluster is HTCondorCluster or Cluster is LocalCluster:
        request.node.add_marker(
            pytest.mark.xfail(
                reason="%s has a peculiar submit script and does not have a shebang"
                % type(Cluster).__name__
            )
        )
    default_shebang = "#!/usr/bin/env bash"
    python_shebang = "#!/usr/bin/python"
    with Cluster(cores=2, memory="4GB", shebang=python_shebang) as cluster:
        job_script = cluster.job_script()
        assert job_script.startswith(python_shebang)
        assert "bash" not in job_script
    with Cluster(cores=2, memory="4GB") as cluster:
        job_script = cluster.job_script()
        assert job_script.startswith(default_shebang)


def test_dashboard_link(Cluster):
    with Cluster(cores=1, memory="1GB") as cluster:
        assert re.match(r"http://\d+\.\d+\.\d+.\d+:\d+/status", cluster.dashboard_link)


def test_forward_ip(Cluster):
    ip = "127.0.0.1"
    with Cluster(
        processes=4,
        cores=8,
        memory="28GB",
        name="dask-worker",
        scheduler_options={"host": ip},
    ) as cluster:
        assert cluster.scheduler.ip == ip

    default_ip = socket.gethostbyname("")
    with Cluster(processes=4, cores=8, memory="28GB", name="dask-worker") as cluster:
        assert cluster.scheduler.ip == default_ip


@pytest.mark.parametrize("Cluster", [])
@pytest.mark.parametrize(
    "qsub_return_string",
    [
        "{job_id}.admin01",
        "Request {job_id}.asdf was sumbitted to queue: standard.",
        "sbatch: Submitted batch job {job_id}",
        "{job_id};cluster",
        "Job <{job_id}> is submitted to default queue <normal>.",
        "{job_id}",
    ],
)
def test_job_id_from_qsub_legacy(Cluster, qsub_return_string):
    original_job_id = "654321"
    qsub_return_string = qsub_return_string.format(job_id=original_job_id)
    with Cluster(cores=1, memory="1GB") as cluster:
        assert original_job_id == cluster._job_id_from_submit_output(qsub_return_string)


@pytest.mark.parametrize("job_cls", [SGEJob])
@pytest.mark.parametrize(
    "qsub_return_string",
    [
        "{job_id}.admin01",
        "Request {job_id}.asdf was sumbitted to queue: standard.",
        "sbatch: Submitted batch job {job_id}",
        "{job_id};cluster",
        "Job <{job_id}> is submitted to default queue <normal>.",
        "{job_id}",
    ],
)
def test_job_id_from_qsub(job_cls, qsub_return_string):
    original_job_id = "654321"
    qsub_return_string = qsub_return_string.format(job_id=original_job_id)
    job = job_cls(cores=1, memory="1GB")
    assert original_job_id == job._job_id_from_submit_output(qsub_return_string)


@pytest.mark.parametrize("Cluster", [])
def test_job_id_error_handling_legacy(Cluster):
    # non-matching regexp
    with Cluster(cores=1, memory="1GB") as cluster:
        with pytest.raises(ValueError, match="Could not parse job id"):
            return_string = "there is no number here"
            cluster._job_id_from_submit_output(return_string)

    # no job_id named group in the regexp
    with Cluster(cores=1, memory="1GB") as cluster:
        with pytest.raises(ValueError, match="You need to use a 'job_id' named group"):
            return_string = "Job <12345> submitted to <normal>."
            cluster.job_id_regexp = r"(\d+)"
            cluster._job_id_from_submit_output(return_string)


@pytest.mark.parametrize("job_cls", [SGEJob])
def test_job_id_error_handling(job_cls):
    # non-matching regexp
    job = job_cls(cores=1, memory="1GB")
    with pytest.raises(ValueError, match="Could not parse job id"):
        return_string = "there is no number here"
        job._job_id_from_submit_output(return_string)

    # no job_id named group in the regexp
    job = job_cls(cores=1, memory="1GB")
    with pytest.raises(ValueError, match="You need to use a 'job_id' named group"):
        return_string = "Job <12345> submitted to <normal>."
        job.job_id_regexp = r"(\d+)"
        job._job_id_from_submit_output(return_string)


def test_log_directory(Cluster, tmpdir):
    shutil.rmtree(tmpdir.strpath, ignore_errors=True)
    with Cluster(cores=1, memory="1GB"):
        assert not os.path.exists(tmpdir.strpath)

    with Cluster(cores=1, memory="1GB", log_directory=tmpdir.strpath):
        assert os.path.exists(tmpdir.strpath)


def test_cluster_has_cores_and_memory(Cluster):
    base_regex = r"{}.+".format(Cluster.__name__)
    with pytest.raises(ValueError, match=base_regex + r"cores=\d, memory='\d+GB'"):
        Cluster()

    with pytest.raises(ValueError, match=base_regex + r"cores=\d, memory='1GB'"):
        Cluster(memory="1GB")

    with pytest.raises(ValueError, match=base_regex + r"cores=4, memory='\d+GB'"):
        Cluster(cores=4)


@pytest.mark.asyncio
async def test_config_interface():
    net_if_addrs = psutil.net_if_addrs()
    interface = list(net_if_addrs.keys())[0]
    with dask.config.set({"jobqueue.local.interface": interface}):
        cluster = LocalCluster(cores=1, memory="2GB", asynchronous=True)
        await cluster
        expected = "'interface': {!r}".format(interface)
        assert expected in str(cluster.scheduler_spec)
        cluster.scale(1)
        assert expected in str(cluster.worker_spec)


# TODO where to put these tests
def test_job_without_config_name():
    class MyJob(Job):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

    with pytest.raises(ValueError, match="config_name.+MyJob"):
        MyJob(cores=1, memory="1GB")

    class MyJobWithNoneConfigName(MyJob):
        config_name = None

    with pytest.raises(ValueError, match="config_name.+MyJobWithNoneConfigName"):
        MyJobWithNoneConfigName(cores=1, memory="1GB")

    with pytest.raises(ValueError, match="config_name.+MyJobWithNoneConfigName"):
        JobQueueCluster(job_cls=MyJobWithNoneConfigName, cores=1, memory="1GB")


def test_cluster_without_job_cls():
    class MyCluster(JobQueueCluster):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

    with pytest.raises(ValueError, match="job_cls.+MyCluster"):
        MyCluster(cores=1, memory="1GB")


def test_default_number_of_worker_processes(Cluster):
    with Cluster(cores=4, memory="4GB") as cluster:
        assert " --nworkers 4" in cluster.job_script()
        assert " --nthreads 1" in cluster.job_script()

    with Cluster(cores=6, memory="4GB") as cluster:
        assert " --nworkers 3" in cluster.job_script()
        assert " --nthreads 2" in cluster.job_script()


def get_interface_and_port(index=0):
    net_if_addrs = psutil.net_if_addrs()
    interface = list(net_if_addrs.keys())[index]
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((net_if_addrs[interface][0].address, 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = s.getsockname()[1]
        s.close()
    return (interface, port)


def test_scheduler_options(Cluster):
    interface, port = get_interface_and_port()

    with Cluster(
        cores=1,
        memory="1GB",
        scheduler_options={"interface": interface, "port": port},
    ) as cluster:
        scheduler_options = cluster.scheduler_spec["options"]
        assert scheduler_options["interface"] == interface
        assert scheduler_options["port"] == port


def test_scheduler_options_interface(Cluster):
    scheduler_interface, _ = get_interface_and_port()
    worker_interface = "worker-interface"
    scheduler_host = socket.gethostname()

    with Cluster(cores=1, memory="1GB", interface=scheduler_interface) as cluster:
        scheduler_options = cluster.scheduler_spec["options"]
        worker_options = cluster.new_spec["options"]
        assert scheduler_options["interface"] == scheduler_interface
        assert worker_options["interface"] == scheduler_interface

    with Cluster(
        cores=1,
        memory="1GB",
        interface=worker_interface,
        scheduler_options={"interface": scheduler_interface},
    ) as cluster:
        scheduler_options = cluster.scheduler_spec["options"]
        worker_options = cluster.new_spec["options"]
        assert scheduler_options["interface"] == scheduler_interface
        assert worker_options["interface"] == worker_interface

    with Cluster(
        cores=1,
        memory="1GB",
        interface=worker_interface,
        scheduler_options={"host": scheduler_host},
    ) as cluster:
        scheduler_options = cluster.scheduler_spec["options"]
        assert scheduler_options.get("interface") is None
        assert scheduler_options["host"] == scheduler_host
        assert worker_options["interface"] == worker_interface


def test_cluster_error_scheduler_arguments_should_use_scheduler_options(Cluster):
    scheduler_host = socket.gethostname()
    message_template = "pass {!r} through 'scheduler_options'"

    message = message_template.format("host")
    with pytest.raises(ValueError, match=message):
        with Cluster(cores=1, memory="1GB", host=scheduler_host):
            pass

    message = message_template.format("dashboard_address")
    with pytest.raises(ValueError, match=message):
        with Cluster(Cluster, cores=1, memory="1GB", dashboard_address=":8787"):
            pass


def test_import_scheduler_options_from_config(Cluster):
    config_scheduler_interface, config_scheduler_port = get_interface_and_port()

    pass_scheduler_interface, _ = get_interface_and_port(1)

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


def test_wrong_parameter_error(Cluster):
    match = re.compile(
        "unexpected keyword argument.+wrong_parameter.+"
        "{}.+job_kwargs.+cores.+memory.+"
        "wrong_parameter.+wrong_parameter_value".format(Cluster.__name__),
        re.DOTALL,
    )
    with pytest.raises(ValueError, match=match):
        Cluster(cores=1, memory="1GB", wrong_parameter="wrong_parameter_value")


@pytest.mark.filterwarnings("error:Using a temporary security object:UserWarning")
def test_security(EnvSpecificCluster, loop):
    # Shared space configured in all docker compose CIs, fallback to current dir if does not exist (LocalCluster)
    dirname = os.environ.get("CI_SHARED_SPACE", os.getcwd())
    # Copy security files into the shared folder
    test_dir = os.path.dirname(__file__)
    shutil.copy2(os.path.join(test_dir, "key.pem"), dirname)
    shutil.copy2(os.path.join(test_dir, "ca.pem"), dirname)
    key = os.path.join(dirname, "key.pem")
    cert = os.path.join(dirname, "ca.pem")
    security = Security(
        tls_ca_file=cert,
        tls_scheduler_key=key,
        tls_scheduler_cert=cert,
        tls_worker_key=key,
        tls_worker_cert=cert,
        tls_client_key=key,
        tls_client_cert=cert,
        require_encryption=True,
    )

    with EnvSpecificCluster(
        cores=1,
        memory="500MiB",
        security=security,
        protocol="tls",
        loop=loop,
    ) as cluster:
        assert cluster.security == security
        assert cluster.scheduler_spec["options"]["security"] == security
        job_script = cluster.job_script()
        assert "tls://" in job_script
        assert "--tls-key {}".format(key) in job_script
        assert "--tls-cert {}".format(cert) in job_script
        assert "--tls-ca-file {}".format(cert) in job_script

        cluster.scale(jobs=1)

        with Client(cluster, security=security) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = future.result(timeout=30)
            assert result == 11

    with EnvSpecificCluster(
        cores=1,
        memory="100MB",
        security=security,
    ) as cluster:
        assert "tls://" in job_script


def test_security_temporary(EnvSpecificCluster, loop):
    # Shared space configured in all docker compose CIs, fallback to current dir if does not exist (LocalCluster)
    dirname = os.environ.get("CI_SHARED_SPACE", os.getcwd())
    with EnvSpecificCluster(
        cores=1,
        memory="500MiB",
        security=Security.temporary(),
        shared_temp_directory=dirname,
        protocol="tls",
        loop=loop,
    ) as cluster:
        assert cluster.security
        assert cluster.scheduler_spec["options"]["security"] == cluster.security
        job_script = cluster.job_script()
        assert "tls://" in job_script
        keyfile = re.findall(r"--tls-key (\S+)", job_script)[0]
        assert (
            os.path.exists(keyfile)
            and os.path.basename(keyfile).startswith(".dask-jobqueue.worker.key")
            and os.path.dirname(keyfile) == dirname
        )
        certfile = re.findall(r"--tls-cert (\S+)", job_script)[0]
        assert (
            os.path.exists(certfile)
            and os.path.basename(certfile).startswith(".dask-jobqueue.worker.cert")
            and os.path.dirname(certfile) == dirname
        )
        cafile = re.findall(r"--tls-ca-file (\S+)", job_script)[0]
        assert (
            os.path.exists(cafile)
            and os.path.basename(cafile).startswith(".dask-jobqueue.worker.ca_file")
            and os.path.dirname(cafile) == dirname
        )

        cluster.scale(jobs=1)
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = future.result(timeout=30)
            assert result == 11

    # TODO assert not any([os.path.exists(f) for f in [keyfile, certfile, cafile]])


@pytest.mark.xfail_env(
    {"htcondor": "Submitting user do not have a shared home directory in CI"}
)
@pytest.mark.xfail_env(
    {"slurm": "Submitting user do not have a shared home directory in CI"}
)
def test_security_temporary_defaults(EnvSpecificCluster, loop):
    # test automatic behaviour if security is true and shared_temp_directory not set
    with pytest.warns(UserWarning, match="shared_temp_directory"), EnvSpecificCluster(
        cores=1,
        memory="500MiB",
        security=True,
        protocol="tls",
        loop=loop,  # for some reason (bug?) using the loop fixture requires using a new test case
    ) as cluster:
        assert cluster.security
        assert cluster.scheduler_spec["options"]["security"] == cluster.security
        job_script = cluster.job_script()
        assert "--tls-key" in job_script
        assert "--tls-cert" in job_script
        assert "--tls-ca-file" in job_script

        cluster.scale(jobs=1)
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = future.result(timeout=30)
            assert result == 11
