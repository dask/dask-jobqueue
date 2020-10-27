import dask
from dask_jobqueue.core import Job, JobQueueCluster, job_parameters, cluster_parameters
from distributed import SchedulerPlugin, versions
from distributed.deploy.spec import NoOpAwaitable
from distributed.utils import format_dashboard_link

import logging
import json
import os
import pwd
import socket

from packaging import version

from tornado.ioloop import PeriodicCallback

import requests
from requests.auth import HTTPBasicAuth

import math

import gettext
from urllib.parse import urlparse

_ = gettext.gettext

logger = logging.getLogger(__name__)

conductor_cluster_parameters = """manager_rest_address: str
        The Dask manager service REST URL. For example: https://daskmanager.hostname.com:10000
        Specify this value, or specify the ascd_rest_address and instance_group parameters
        to look up this value by the instance group name

    ascd_rest_address: str
        The ASCD REST URL. For example: https://ascd.example.hostname.com:8643/platform/rest/conductor/v1
        Optionally specify this value to look up the Dask manager service REST URL by instance group name.
        Alternatively, set the ``CONDUCTOR_REST_URL`` environment variable.
    ascd_rest_cacert_path: str
        Path to the CA certificate file for the IBM Spectrum Conductor cluster.
        Required to look up the Dask manager service REST URL by instance group name when SSL is enabled
        for the IBM Spectrum Conductor cluster, and when SSL is enabled for the Dask manager service.
        Alternatively, set the ``ASCD_REST_CACERT_PATH`` environment variable.
    instance_group: str
        Name of the instance group on the IBM Spectrum Conductor cluster.
        Optionally specify this value to look up the Dask manager service REST URL by instance group name.
    username: str
        Username for the IBM Spectrum Conductor cluster. Required to look up the Dask manager service REST URL
        by instance group name, or when authentication is enabled for the instance group.
        Alternatively, set the ``EGO_USERNAME`` or ``EGO_SERVICE_CREDENTIAL`` environment variables.
    password: str
        Password for the IBM Spectrum Conductor cluster. Required to look up the Dask manager service REST URL
        by instance group name, or when authentication is enabled for the instance group.
        Alternatively, set the ``EGO_PASSWORD`` or ``EGO_SERVICE_CREDENTIAL`` environment variables.
    adapt: bool
        Indicates whether the workers will automatically be scaled according to workload
    min_workers: int
        Indicates the minimum number of workers.
    max_workers: int
        Indicates the maximum number of workers.
    type: str
        The type of Dask worker to start: CPU or GPU.
    gpu_mode: str
        The GPU mode to use for Dask workers: DEFAULT or EXCLUSIVE.
        This parameter is ignored when type=CPU.
    worker_command: str
        The command used to start the Dask worker.
        If not specified, dask-worker will be used for CPU workload and dask-cuda-worker will be used for GPU workload.
    slots: str
        The number of slots on the IBM Spectrum Conductor cluster used by each ``ConductorJob``.
        Each ConductorJob represents one EGO activity within the IBM Spectrum Conductor cluster.
    conda_env_path: str
        The path to the conda environment to use to run the Dask workers.
    job_max_retries: str
        The maximum number of EGO activities to request when trying to start a ``ConductorJob``.
    failed_job_limit: int
        Indicates the maximum number of times a ``ConductorJob`` can fail to start before it is cancelled.
"""


def log_rest_error(resp, level=logging.ERROR):
    msg = None
    try:
        msg = _("The Dask manager returned an error. Code: %d. Message: %s") % (
            resp.status_code, resp.json()['message'])
    except Exception:
        msg = _("The Dask manager returned an error. Code: %d.") % (resp.status_code)

    if level == logging.ERROR:
        logger.error(msg)
    elif level == logging.WARNING:
        logger.warning(msg)
    elif level == logging.INFO:
        logger.info(msg)
    elif level == logging.DEBUG:
        logger.debug(msg)


def get_activity_id_from_worker_name(worker_name):
    """
        Extract the activity ID from the worker name.
        Worker name is expected to be in the oneof the following formats:
            Worker__hostname.example.com__Activity-XXX
            Worker__hostname.example.com__Activity-XXX-Y
        Where XXX is the activity ID.
    """
    activity_id = None
    activity_prefix = '__Activity-'
    i = worker_name.rindex(activity_prefix)
    if (i + len(activity_prefix)) != len(worker_name):
        activity_id = worker_name[i + len(activity_prefix):]
        if "-" in activity_id:
            activity_id = activity_id[: activity_id.rindex("-")]
    return activity_id


class ConductorJob(Job):
    submit_command = None
    cancel_command = None
    config_name = "conductor"

    def __init__(
        self,
        *args,
        scheduler=None,
        config_name=None,
        **kwargs
    ):
        super().__init__(*args, config_name=config_name, cores=-1, memory=-1, shebang="", **kwargs)
        self._max_retries = int(dask.config.get("jobqueue.%s.job-max-retries" % config_name, 5))

    async def start(self):
        await self._submit_job(None)
        self.job_id = "NOT_STARTED"
        self.status = "running"   # dask-jobqueue framework expects status to be "running" after call to this method.
        self.nprocs = 0
        self.failed_activities = []

    async def close(self):
        """
            Override Job close method to do nothing. Workers will close by themselves and be handled
            through EGO callback mechanism.
        """
        if self.status == "running" and self.job_id == "NOT_STARTED":
            # For jobs that are not started, notify the Dask manager that they are no longer needed
            # Jobs that are already started will be cleaned up by the Dask manager automatically
            logger.debug("Close a job that was not started. Requested number of workers is reduced by 1.")
            self._send_dask_manager_workers_rest_request(num_workers=-1)
        else:
            self.status = "closed"

    async def _submit_job(self, script_filename):
        """
            Submit a job to the Conductor cluster
        """
        logger.debug("Submit a new job.")
        self._send_dask_manager_workers_rest_request(num_workers=1)

    def update_name_and_id(self, worker_name):
        """
            Update the job name and job_id.
            The job name will be set to the worker name.
            The job_id will be set to the EGO activity ID for the worker activity.
        """
        self.job_id = get_activity_id_from_worker_name(worker_name)
        self.nprocs += 1
        if worker_name.endswith(self.job_id):
            self.name = worker_name
        else:
            self.name = worker_name[:worker_name.rindex("-")]

    def cancel_job(self):
        """
            Cancel the Job without contacting the Dask manager
        """
        if self.job_id == "NOT_STARTED":
            logger.debug("Cancel a job that was not started.")
        else:
            logger.debug("Cancel the job with ID %s.", self.job_id)

        self.status = "closed"

    def fail_job(self, activity_id):
        self.failed_activities.append(activity_id)
        if len(self.failed_activities) >= self._max_retries:
            self.status = "failed"
            logger.error(_("The job could not start any workers after %d tries. "
                           "See worker logs for the following activity IDs for more details: "
                           "%s"), len(self.failed_activities), str(self.failed_activities))
            return True
        else:
            logger.debug("Job failed to start worker. Request a new worker.")
            self._send_dask_manager_workers_rest_request(num_workers=1)
            return False

    def _send_dask_manager_workers_rest_request(self, num_workers=0):
        """
            Send a PUT /dask/manager/schedulers/{scheduler-id}/workers request to the dask manager

            Parameters:
            -----------
            num_workers: int
                Number of workers to request (positive int), or release (negative int).
        """
        if num_workers == 0:
            return

        manager_rest_address = dask.config.get("jobqueue.conductor_internal.manager_rest_address")
        scheduler_id = dask.config.get("jobqueue.conductor_internal.scheduler_id")
        cookies = dask.config.get("jobqueue.conductor_internal.saved_cookies", None)
        csrftoken = dask.config.get("jobqueue.conductor_internal.csrftoken", None)
        cacert = False
        if manager_rest_address.startswith("https"):
            cacert = dask.config.get("jobqueue.conductor_internal.cacert_path", False)

        headers = {"Accept": "application/json",
                   "Content-Type": "application/json"}

        url = manager_rest_address + "/dask/manager/schedulers/" + scheduler_id + "/workers"
        if csrftoken is not None:
            url = url + "?csrftoken=" + csrftoken

        data = {"numworkers": num_workers}

        resp = requests.put(url, data=str(data), headers=headers, cookies=cookies, verify=cacert)

        if resp.status_code != requests.codes.no_content:
            if num_workers < 0 and resp.status_code == requests.codes.conflict:
                # If the Dask manager has already started the activity for this job, it will return 409 Conflict
                # We do not need to raise the exception. The worker will be closed when it tries to connect
                log_rest_error(resp, logging.DEBUG)
            else:
                log_rest_error(resp)
                resp.raise_for_status()


class ConductorCluster(JobQueueCluster):
    __doc__ = """
    Cluster that runs on IBM Spectrum Conductor.

    The ``ConductorCluster`` class interfaces with the Dask manager service running in an instance group on an
    IBM Spectrum Conductor cluster. If initialized on a host inside the IBM Spectrum Conductor cluster,
    the configuration from the deployed instance group will be used by default.

    Parameters:
    -----------
    {conductor_cluster}
    {job}
    {cluster}

    Examples:
    ----------
    To create a ``ConductorCluster`` from within an integrated notebook on the IBM Spectrum Conductor cluster,
    no arguments are required.
    All configuration options with be automatically set according to the instance group configuration:

    >> from daskconductor.conductor import ConductorCluster
    >> cluster = ConductorCluster()

    To create a ``ConductorCluster`` when you know the Dask manager service URL:

    >> from daskconductor.conductor import ConductorCluster
    >> cluster = ConductorCluster(manager_rest_address="https://daskmanager.example.com:10000/")

    To create a ``ConductorCluster`` when you know the instance group name:

    >> from daskconductor.conductor import ConductorCluster
    >> cluster = ConductorCluster(instance_group_name="ig1",
        ascd_rest_address="http://ascd.example.com:8280/platform/rest/conductor/v1",
        username="MyUser", password="MyPassword")

    """.format(
        conductor_cluster=conductor_cluster_parameters, job=job_parameters, cluster=cluster_parameters
    )

    job_cls = ConductorJob
    config_name = "conductor"

    def __init__(
        self,
        # Cluster keywords
        loop=None,
        security=None,
        silence_logs=logging.ERROR,
        name=None,
        asynchronous=False,
        manager_rest_address=None,
        ascd_rest_address=None,
        ascd_rest_cacert_path=None,
        instance_group=None,
        username=None,
        password=None,
        adapt=None,
        min_workers=None,
        max_workers=None,
        n_workers=None,
        failed_job_limit=None,
        # Scheduler keywords
        interface=None,
        host=None,
        protocol=None,
        dashboard_address=":8787",
        config_name=None,
        log_directory=None,
        # Job keywords
        type=None,
        worker_command=None,
        slots=None,
        threads=None,
        memlimit=None,
        resources=None,
        local_directory=None,
        conda_env_path=None,
        lifetime=None,
        processes=None,
        gpu_mode=None,
        extra=None,
        env_extra=None,
        job_max_retries=None,
        **kwargs
    ):

        # Get the worker parameters from the config if they are not explicitly specified
        if n_workers is None:
            n_workers = dask.config.get("jobqueue.%s.start-workers" % self.config_name, None)
            if n_workers is None:
                n_workers = 0

        if type is None:
            type = dask.config.get("jobqueue.%s.type" % self.config_name, None)

        if worker_command is None:
            worker_command = dask.config.get("jobqueue.%s.worker_command" % self.config_name, None)

        if slots is None:
            slots = dask.config.get("jobqueue.%s.slots" % self.config_name, None)

        if threads is None:
            threads = dask.config.get("jobqueue.%s.threads" % self.config_name, None)

        if memlimit is None:
            memlimit = dask.config.get("jobqueue.%s.memlimit" % self.config_name, None)

        if resources is None:
            resources = dask.config.get("jobqueue.%s.resources" % self.config_name, None)

        if processes is None:
            processes = dask.config.get("jobqueue.%s.processes" % self.config_name, None)

        if gpu_mode is None:
            gpu_mode = dask.config.get("jobqueue.%s.gpu-mode" % self.config_name, None)

        if interface is None:
            interface = dask.config.get("jobqueue.%s.interface" % self.config_name, None)

        if conda_env_path is None:
            conda_env_path = dask.config.get("jobqueue.%s.conda-env-path" % self.config_name, None)

        if local_directory is None:
            local_directory = dask.config.get("jobqueue.%s.local-directory" % self.config_name, None)

        if lifetime is None:
            lifetime = dask.config.get("jobqueue.%s.lifetime" % self.config_name, None)

        if extra is None:
            extra = dask.config.get("jobqueue.%s.extra" % self.config_name, None)
        if extra is not None and not isinstance(extra, list):
            raise ValueError(_("The value '%s' specified for the 'extra' parameter is not supported. "
                               "Specify a list of arguments to pass to the Dask worker. "
                               " For example: ['--extra1', '--extra2 arg2'].") % extra)

        if env_extra is None:
            env_extra = dask.config.get("jobqueue.%s.env_extra" % self.config_name, None)
        if env_extra is not None and not isinstance(env_extra, list):
            raise ValueError(_("The value '%s' specified for the 'env_extra' parameter is not supported. "
                               "Specify a list of environment variables for the Dask worker. "
                               "For example: ['ENV1=val1','ENV2=val2'].))") % env_extra)

        if log_directory is None:
            log_directory = dask.config.get("jobqueue.%s.log-directory" % self.config_name, None)

        # Get other cluster parameters from conf file if not explicitly specified
        if failed_job_limit is None:
            self._failed_job_limit = int(dask.config.get("jobqueue.%s.failed-job-limit" % self.config_name, 5))
        else:
            self._failed_job_limit = int(failed_job_limit)

        # Build worker spec for the scheduler
        self.worker_specs = {}
        if type is not None:
            self.worker_specs['type'] = type

        if name is not None:
            if type is None or type == "CPU":
                self.worker_specs['cpucommand'] = worker_command
            else:
                self.worker_specs['gpucommand'] = worker_command
                if gpu_mode is not None:
                    self.worker_specs['gpumode'] = gpu_mode

        if threads is not None:
            self.worker_specs['numthreads'] = int(threads)

        if memlimit is not None:
            self.worker_specs['memlimit'] = memlimit

        if processes is not None:
            self.worker_specs['numprocs'] = int(processes)

        if resources is not None:
            self.worker_specs['resources'] = resources

        if interface is not None:
            self.worker_specs['networkinterface'] = interface

        if lifetime is not None:
            self.worker_specs['lifetime'] = int(lifetime)

        if extra is not None:
            self.worker_specs['extra'] = extra

        if env_extra is not None:
            self.worker_specs['envextra'] = env_extra

        if conda_env_path is not None:
            self.worker_specs['condaenvpath'] = conda_env_path

        if local_directory is not None:
            self.worker_specs['localdirectory'] = local_directory

        if slots is not None:
            self._slots_per_worker = int(slots)
            self.worker_specs['slots'] = self._slots_per_worker

        self.log_directory = log_directory

        # Initialize User Credentials
        # Note that username and password are shared between ASCD and Dask manager REST
        self._username = username
        self._password = password
        self._credentials = None
        self._init_user_credentials()

        # Initialize the Dask manager REST address
        self.ascd_rest_address = ascd_rest_address
        self.ascd_rest_cacert_path = ascd_rest_cacert_path
        self.instance_group = instance_group

        self.manager_rest_address = manager_rest_address
        if self.manager_rest_address is None:
            self._look_up_manager_rest_address()
            if self.manager_rest_address is None:
                raise ValueError(_("The Dask manager REST address was not specified, "
                                   "and could not be looked up using the ascd REST service."))

        # Call the parent constructor to create the cluster
        try:
            super(ConductorCluster, self).__init__(loop=loop,
                                                   security=security,
                                                   silence_logs=silence_logs,
                                                   name=name,
                                                   asynchronous=asynchronous,
                                                   interface=interface,
                                                   host=host,
                                                   protocol=protocol,
                                                   dashboard_address=dashboard_address,
                                                   config_name=config_name,
                                                   # We will start the n_workers after registering with Dask manager
                                                   n_workers=0,
                                                   **kwargs)
        except ValueError:
            # In dask-jobqueue 0.7.1 there is a breaking change where dashboard_address must be passed through a new
            # scheduler_options parameter. The scheduler-options parameter can also be defined in the jobqueue.yaml
            # If it is not there, we will fill scheduler_options with the old parameter values
            scheduler_options = dask.config.get("jobqueue.%s.scheduler-options" % self.config_name, None)
            if scheduler_options is None or scheduler_options == {}:
                scheduler_options = {'dashboard_address': dashboard_address,
                                     'interface': interface,
                                     'host': host,
                                     'security': security,
                                     'protocol': protocol}

            super(ConductorCluster, self).__init__(loop=loop,
                                                   security=security,
                                                   silence_logs=silence_logs,
                                                   name=name,
                                                   asynchronous=asynchronous,
                                                   config_name=config_name,
                                                   scheduler_options=scheduler_options,
                                                   # We will start the n_workers after registering with Dask manager
                                                   n_workers=0,
                                                   **kwargs)

        # At this point we have a scheduler id, so re-initialize the logs with the new filename
        self._reinitialize_logging_with_scheduler_id()

        # If there is a failure initializig after this point, we need to close the cluster
        self._callbacks = []
        try:
            # Set up error handling variables
            self._unable_to_start_jobs_count = 0

            # Set dask config for this instance to pass these values to the ConductorJob
            dask.config.set({'jobqueue.conductor_internal.manager_rest_address': self.manager_rest_address,
                             'jobqueue.conductor_internal.scheduler_id': self.scheduler.id,
                             'jobqueue.conductor_internal.cacert_path': self.ascd_rest_cacert_path
                             })

            if job_max_retries is not None:
                dask.config.set({'jobqueue.%s.job-max-retries' % config_name: job_max_retries})

            # Register the plugin
            plugin = ConductorPlugin(scheduler=self.scheduler, cluster=self,
                                     manager_rest_address=self.manager_rest_address)
            self.scheduler.add_plugin(plugin)

            # Initialize authentication info
            self._initial_logon_dask_manager()

            # Register with the manager service
            self._register_scheduler_with_manager()

            # Get the cluster parameters from the conf id they are not explicitly specified
            if adapt is None:
                adapt = dask.config.get("jobqueue.%s.adapt" % self.config_name, None)
                if adapt is not None:
                    adapt = bool(adapt)

            if min_workers is None:
                min_workers = dask.config.get("jobqueue.%s.min-workers" % self.config_name, None)
                if min_workers is not None:
                    min_workers = int(min_workers)

            if max_workers is None:
                max_workers = dask.config.get("jobqueue.%s.max-workers" % self.config_name, None)
                if max_workers is not None:
                    max_workers = int(max_workers)

            if n_workers is not None and n_workers != 0:
                self.scale(n_workers)

            if adapt:
                self.adapt(minimum_jobs=min_workers, maximum_jobs=max_workers)

            for callback in self._callbacks:
                callback.start()

        except Exception as e:
            logger.exception(e)
            self.close()

    def _reinitialize_logging_with_scheduler_id(self):
        """
            Re-initialize logging to move scheduler logs to the right folder
        """
        try:
            log_conf = dask.config.get("conductorlogging")
            hostname = socket.getfqdn()
            username = pwd.getpwuid(os.getuid()).pw_name
            orig_filename = log_conf.get("handlers").get("file").get("filename")
            new_filename = self.scheduler.id + "__" + hostname + "__" + username + ".log"
            new_log_dir = None
            if self.log_directory is not None:
                new_log_dir = os.path.join(self.log_directory, self.scheduler.id)
            elif "/" in orig_filename:
                self.log_directory = orig_filename[:orig_filename.rindex("/")]
                new_log_dir = os.path.join(self.log_directory, self.scheduler.id)
                dask.config.set({'jobqueue.%s.log-directory' % (self.config_name): self.log_directory})
            else:
                self.log_directory = os.getcwd()
                new_log_dir = os.path.join(self.log_directory, self.scheduler.id)
                dask.config.set({'jobqueue.%s.log-directory' % (self.config_name): self.log_directory})

            if not os.path.exists(new_log_dir):
                os.mkdir(new_log_dir)

            new_filepath = os.path.join(new_log_dir, new_filename)
            log_conf["handlers"]["file"]["filename"] = new_filepath
            logging.config.dictConfig(log_conf)

            # Give the new directory with group write permissions
            # This is to handle the case of multiple users when impersonation is disabled
            os.chmod(new_log_dir, 0o770)

            # Re-log the scheduler address and dashboard address in the new log
            logger.info(_("ConductorCluster initialized. Scheduler: %s, Dashboard: %s"),
                        self.scheduler.address, self.dashboard_link)

        except KeyError:
            # The logging is configured differently, so leave it alone
            pass

    def _initial_logon_dask_manager(self):
        """
            Perform logon and set tokens
        """

        self._csrftoken = None
        self._saved_cookies = None
        self.default_headers = {"Accept" : "application/json",
                                "Content-Type" : "application/json"}
        self.do_auth = False

        headers = self.default_headers.copy()
        auth = None

        if self._username is not None and self._password is not None:
            auth = HTTPBasicAuth(self._username, self._password)
            self.do_auth = True
        elif self._credentials is not None:
            headers.update({"Authorization": "PlatformToken token=" + self._credentials})
            self.do_auth = True

        if self.do_auth:
            auth_url = self.manager_rest_address + "/dask/manager/auth/logon"
            resp = self._submit_dask_manager_rest_request("GET", auth_url, headers=headers, auth=auth)
            if resp.status_code != requests.codes.ok:
                log_rest_error(resp)
                resp.raise_for_status()
            else:
                self._csrftoken = resp.json()['csrftoken']
                self._saved_cookies = resp.cookies

                # Use the dask config to push these values to the ConductorJob
                dask.config.set({'jobqueue.conductor_internal.saved_cookies': self._saved_cookies,
                                 'jobqueue.conductor_internal.csrftoken': self._csrftoken
                                 })
                # Set callback to renew token once every hour
                self._callbacks.append(PeriodicCallback(self._renew_token, 3600000))

    def _renew_token(self):
        """
            Renew the token for authentication with the Dask manager service
        """
        if self.do_auth:
            renew_url = self.manager_rest_address + "/dask/manager/auth/renew"
            resp = self._submit_dask_manager_rest_request("POST", renew_url)
            if resp.status_code != requests.codes.ok:
                log_rest_error(resp)
                resp.raise_for_status()
            else:
                self._csrftoken = resp.json()['csrftoken']
                self._saved_cookies['platform.rest.token'] = resp.cookies['platform.rest.token']

                # Push to dask config for ConductorJobs
                dask.config.set({'jobqueue.conductor_internal.saved_cookies': self._saved_cookies,
                                 'jobqueue.conductor_internal.csrftoken': self._csrftoken
                                 })

    def _register_scheduler_with_manager(self):
        """
            Register a new scheduler with the Dask manager service.
        """

        os_username = pwd.getpwuid(os.getuid()).pw_name
        url = self.manager_rest_address + "/dask/manager/schedulers"

        data = {"id": self.scheduler.id,
                "address": self.scheduler.address,
                "username": os_username,
                "dashboard": self.dashboard_link,
                "worker": self.worker_specs}

        resp = self._submit_dask_manager_rest_request("POST", url, data=data)
        if resp.status_code != requests.codes.ok:
            log_rest_error(resp)
            resp.raise_for_status()

    def _unregister_scheduler_from_manager(self, ignore_rest_errors=True):
        """
            Unregister the scheduler with the Dask manager service.
        """
        url = self.manager_rest_address + "/dask/manager/schedulers/" + self.scheduler.id

        resp = self._submit_dask_manager_rest_request("DELETE", url)
        if resp.status_code != requests.codes.no_content:
            if ignore_rest_errors:
                log_rest_error(resp, logging.DEBUG)
            else:
                log_rest_error(resp)
                resp.raise_for_status()

    def _submit_dask_manager_rest_request(self, method, url, data=None, headers={}, auth=None):
        """
            Submit a REST request to the Dask manager.
            The method will handle appending csrftoken and cookies if authentication is enabled.
            The method will also handle sending cacert path if SSL is enabled.


            Parameters:
            -----------
            url: string
                URL for the REST request (csrftoken will be appended automatically)
            method: string
                One of GET, POST, PUT, DELETE
            data: dict
                Optional dict to send as request body
            headers: dict
                Optional extra headers. Accept and Content-Type will be appended automatically
            auth: HTTPBasicAuth
                Optional basic auth info
        """
        headers.update(self.default_headers)

        cacert_path = False
        if self.manager_rest_address.startswith("https"):
            cacert_path = self.ascd_rest_cacert_path

        if data is not None:
            data = str(data)

        if self._csrftoken is not None:
            url = url + "?csrftoken=" + self._csrftoken

        resp = None
        if method == "GET":
            resp = requests.get(url, headers=headers, verify=cacert_path,
                                cookies=self._saved_cookies, auth=auth)
        elif method == "POST":
            resp = requests.post(url, data=data, headers=headers, verify=cacert_path,
                                 cookies=self._saved_cookies, auth=auth)
        elif method == "PUT":
            resp = requests.put(url, data=data, headers=headers, verify=cacert_path,
                                cookies=self._saved_cookies, auth=auth)
        elif method == "DELETE":
            resp = requests.delete(url, data=data, headers=headers, verify=cacert_path,
                                   cookies=self._saved_cookies, auth=auth)

        return resp

    def _init_user_credentials(self):
        """
            Initialize the username/password or credential.
            Note that these values are shared between ASCD and Dask manager REST.

            Initialize authentication. Check the following in order:
                1. Username and password are specified explicitly
                2. The EGO_SERVICE_CREDENTIAL environment variable is specified
                3. The EGO_USERNAME and EGO_PASSWORD environment variables are specified

            If authentication information is found, perform initial logon and save tokens.
        """
        if self._username is None or self._password is None:
            self._credentials = os.environ.get("EGO_SERVICE_CREDENTIAL", None)

        if self._credentials is None:
            if self._username is None:
                self._username = os.environ.get("EGO_USERNAME", None)
            if self._password is None:
                self._password = os.environ.get("EGO_PASSWORD", None)

    def _look_up_manager_rest_address(self):
        """
            Try to look up the Dask manager through the ASCD REST API
        """
        # Validate credentials are provided
        if self._credentials is None and (self._username is None or self._password is None):
            logger.error(_("Could not look up the Dask manager address from the ascd REST service. "
                           "The username, password or credentials were not specified."))
            return None

        # Get the instance group name from args or Dask config
        if self.instance_group is None:
            self.instance_group = dask.config.get("jobqueue.%s.instance-group" % self.config_name, None)
            if self.instance_group is None:
                logger.error(_("Could not look up the Dask manager address from the ascd REST service. "
                               "The instance group name was not specified."))
                return None

        # Get the cacert path from args or environment variable
        if self.ascd_rest_cacert_path is None:
            self.ascd_rest_cacert_path = os.environ.get('ASCD_REST_CACERT_PATH', None)

        attempted_lookup = False

        # Get the ascd_rest_address from the args
        if self.ascd_rest_address is not None:
            url = urlparse(self.ascd_rest_address)
            if url.port is None:
                logger.error(_("The ascd REST address %s does not contain a port number."), self.ascd_rest_address)
            else:
                if url.path == "":
                    self.ascd_rest_address = self.ascd_rest_address + "/platform/rest"

                logger.debug("Trying to look up the Dask manager address for the instance group %s from the "
                             "ascd REST service at %s" % (self.instance_group, self.ascd_rest_address))
                attempted_lookup = True
                resp = self._submit_ascd_get_instances_rest_call(self.ascd_rest_address)

                if resp is None:
                    logger.error(_("Could n find the Dask manager address for the instance group %s from the "
                                   "ascd REST service at %s."), self.instance_group, self.ascd_rest_address)
                    return None

                if resp is not False:
                    if resp.status_code == requests.codes.ok:
                        self.manager_rest_address = self._parse_dask_manager_address_from_json(resp.json())
                        return
                    else:
                        log_rest_error(resp)
                        resp.raise_for_status()

        # If the ascd_rest_address didn't work, see if CONDUCTOR_REST_URL is defined
        conductor_rest_address = os.environ.get('CONDUCTOR_REST_URL', None)
        if conductor_rest_address is not None:
            logger.debug("Trying to look up the Dask manager address for the instance group %s from the "
                         "ascd REST service at %s" % (self.instance_group, conductor_rest_address))
            attempted_lookup = True
            resp = self._submit_ascd_get_instances_rest_call(conductor_rest_address)

            if resp is None:
                logger.error(_("Could not find the Dask manager address for the instance group %s from the "
                               "ascd REST service at %s."""), self.instance_group, conductor_rest_address)
                return None

            if resp is not False:
                if resp.status_code == requests.codes.ok:
                    self.manager_rest_address = self._parse_dask_manager_address_from_json(resp.json())
                    return
                else:
                    log_rest_error(resp)
                    resp.raise_for_status()

        # If still no luck, try cycling the hosts in MANAGEMENT_HOST_LIST if it is defined
        ascd_hosts = os.environ.get('MANAGEMENT_HOST_LIST', None)
        if ascd_hosts is not None:
            base_url = None
            if self.ascd_rest_address is not None:
                base_url = urlparse(self.ascd_rest_address)
            elif conductor_rest_address is not None:
                base_url = urlparse(conductor_rest_address)
            else:
                logger.error(_("Could not look up the Dask manager address. "
                               "The ascd REST service base URL was not specified."))
                return None

            for host in ascd_hosts.split(","):
                if host != base_url.hostname:
                    next_ascd_host_rest_address = ("%s://%s:%d/platform/rest") % (base_url.scheme, host, base_url.port)

                    logger.debug("Trying to look up the Dask manager address for the instance group %s from the "
                                 "ascd REST service at %s." % (self.instance_group, next_ascd_host_rest_address))
                    attempted_lookup = True
                    resp = self._submit_ascd_get_instances_rest_call(next_ascd_host_rest_address)

                    if resp is None:
                        logger.error(_("Could not find the Dask manager address for the instance group %s from the "
                                       "ascd REST service at %s."), self.instance_group, next_ascd_host_rest_address)
                        return None

                    if resp is not False:
                        if resp.status_code == requests.codes.ok:
                            self.manager_rest_address = self._parse_dask_manager_address_from_json(resp.json())
                            return
                        else:
                            log_rest_error(resp)
                            resp.raise_for_status()

        # If we get here we have exhausted all of the options
        if attempted_lookup:
            logger.error(_("Could not find the Dask manager address for the instance group %s from any ascd host."),
                         self.instance_group)

    def _submit_ascd_get_instances_rest_call(self, base_address):
        """
            Make REST call to /platform/rest/conductor/v1/instances?name=instance_group_name&fields=name,outputs

            Returns:
            --------
            None - Error case
            False - Connection refused error
            Resp - Success case

        """
        base_address = base_address.rstrip('/')

        verify = False
        if self.ascd_rest_cacert_path is None:
            if base_address.startswith("https://"):
                logger.error(_("Could not look up the Dask manager address. The certificate authority "
                               "(CA) certificate file for the ascd REST service was not specified."))
                return None
        else:
            verify = self.ascd_rest_cacert_path

        url = base_address + "/conductor/v1/instances?name=" + self.instance_group + "&fields=name,outputs"

        headers = {"Accept": "application/json",
                   "Content-Type": "application/json"}

        resp = None

        if self._credentials is not None:
            # With the credentials token, we first try to authenticate and get the csrftoken
            auth_url = base_address + "/conductor/v1/auth/logon"

            headers.update({"Authorization": "PlatformToken token=" + self._credentials})
            auth_resp = None
            try:
                auth_resp = requests.get(auth_url, headers=headers, verify=verify)
                if auth_resp.status_code != requests.codes.ok:
                    log_rest_error(resp)
                    # This is a general error case, do not try other ASCD hosts
                    return None
            except Exception as e:
                message = getattr(e, 'message', str(e))
                logger.warn(_("Could not connect to the ascd REST service at %s. %s"), auth_url, message)
                return False

            csrftoken = auth_resp.json()['csrftoken']
            url = url + "&csrftoken=" + csrftoken

            # Now remove the Authorization header and perform the lookup
            del headers['Authorization']
            resp = requests.get(url, headers=headers, cookies=auth_resp.cookies, verify=verify)
            if resp.status_code != requests.codes.ok:
                log_rest_error(resp)
                return None
            else:
                return resp

        elif self._username is not None and self._password is not None:
            auth = HTTPBasicAuth(self._username, self._password)
            try:
                resp = requests.get(url, headers=headers, verify=verify, auth=auth)

                if resp.status_code != requests.codes.ok:
                    log_rest_error(resp)
                    return None
                else:
                    return resp
            except Exception:
                logger.info(_("Could not connect to ASCD at %s." % base_address))
                return False

        else:
            logger.error(_("Could not look up the Dask manager address. "
                           "The username, password, or credentials were not specified."))
            return None

        return None

    def _parse_dask_manager_address_from_json(self, instance_groups):

        if instance_groups is None:
            return None

        for ig in instance_groups:
            if ig.get('name') == self.instance_group:
                outputs = ig.get('outputs')
                if outputs is None:
                    logger.warn(_("Could not look up the Dask manager address. "
                                "The instance group %s does not have a Dask manager service."), self.instance_group)
                    return None

                dask_manager_rest_address = outputs.get('dask_manager_rest_address')
                if dask_manager_rest_address is None:
                    logger.warn(_("Could not look up the Dask manager address. "
                                "The instance group %s does not have a Dask manager service."), self.instance_group)
                    return None

                value = dask_manager_rest_address.get('value')
                if value is None:
                    logger.warn(_("Could not look up the Dask manager address. "
                                "The instance group %s does not have a running Dask manager service instance."),
                                self.instance_group)
                    return None

                # Successfully got the address
                return value.strip('/dask/manager')

        logger.warn(_("Could not look up the Dask manager address. The instance group %s was not found."),
                    self.instance_group)
        return None

    def close(self):
        """
            Close the ConductorCluster
        """
        self._cleanup_cluster()
        super(JobQueueCluster, self).close()

    def _cleanup_cluster(self):
        """
            Clean up the ConductorCluster
        """
        for job in self.workers.values():
            job.cancel_job()

        for callback in self._callbacks:
            callback.stop()

        if self.manager_rest_address:
            self._unregister_scheduler_from_manager()

    async def _close(self):
        """
            Close the ConductorCluster (internal)
        """
        self._cleanup_cluster()
        await super(JobQueueCluster, self)._close()
        self.status = 'closed'

    def scale(self, n=0, jobs=None, slots=None):
        """
            Override scale method for the ConductorCluster.

            Parameters:
            -----------
            n: int
                Number of workers to scale to
            jobs: int
                Number of jobs to scale to - Takes precedence over number of workers
            slots: int
                Number of slots to scale to - Takes precedence over number of jobs
        """

        if slots is not None:
            n = max(n, int(math.ceil(slots / self._slots_per_worker)))

        if jobs is not None:
            n = jobs

        if len(self.worker_spec) > n:
            not_yet_launched = set(self.worker_spec) - set(self.scheduler_info["workers"])
            while len(self.worker_spec) > n and not_yet_launched:
                del self.worker_spec[not_yet_launched.pop()]

        while len(self.worker_spec) > n:
            self.worker_spec.popitem()

        if self.status not in ("closing", "closed"):
            while len(self.worker_spec) < n:
                self.worker_spec.update(self.new_worker_spec())

        self.loop.add_callback(self._correct_state)

        if self.asynchronous:
            return NoOpAwaitable()

    def get_workers_with_job_id(self, job_id="NOT_STARTED"):
        """
            Get the workers with the specified job_id.
        """
        return set(filter(lambda job: job.job_id == job_id, self.workers.values()))

    def update_launched_job(self, worker_address=None, worker_name=None):
        """
            Update the job info with the real job name and worker address once the worker has started

            Parameters:
            -----------
            worker_address: str
                Address of the worker
            worker_name: str
                Name of the worker
        """
        try:
            # Job is started, reset counter
            self._unable_to_start_jobs_count = 0

            matching_jobs = self.get_workers_with_job_id()  # Get workers with NOT_STARTED job id

            if len(matching_jobs) != 0:
                # This is the first worker coming in for this job
                spec_key = matching_jobs.pop().name

                # Replace the key for the launched worker in the cluster.worker_spec
                spec = self.worker_spec[spec_key]
                del self.worker_spec[spec_key]
                self.worker_spec.update({worker_address: spec})

                # Update the ConductorJob name and ID in the cluster.workers
                job = self.workers[spec_key]
                del self.workers[spec_key]
                job.update_name_and_id(worker_name)
                self.workers.update({worker_address: job})
            else:
                activity_id = get_activity_id_from_worker_name(worker_name)
                matching_jobs = set(filter(lambda job: job.job_id == activity_id, self.workers.values()))
                if len(matching_jobs) != 0:
                    job = matching_jobs.pop()
                    if job.status == "running":
                        job.update_name_and_id(worker_name)
                        # Other workers for this job have already been added, so this job must have multiple processes
                        # Add a new worker_spec for the new worker, referencing the existing job
                        spec_key = job.name
                        spec = self.worker_spec[spec_key]
                        self.worker_spec.update({worker_address: spec})

                        # Add a new worker referencing the existing job
                        self.workers.update({worker_address: job})
                    else:
                        logger.warn(_("The job with activity ID %s is not running.") % activity_id)
                else:
                    # We get to this case when the ConductorCluster has scaled down, then receives an incoming worker
                    # previously requested. Retire the worker and return the slot since we no longer need it
                    logger.info(_("There are no jobs waiting to be launched. Closing the Dask worker %s."), worker_name)
                    self.loop.add_callback(self.retire_workers, workers=[worker_address])

        except KeyError:
            # We get into this case when the ConductorCluster closes the pending job at the same time that
            # the worker is being added. Retire the worker and return the slot since we no longer need it
            logger.info(_("There are no jobs waiting to be launched. Closing the Dask worker %s."), worker_name)
            self.loop.add_callback(self.retire_workers, workers=[worker_address])

    async def retire_workers(self, workers=None):
        """
            Retire and close workers from the scheduler and clean them up from the cluster.

            Parameters:
            -----------
            workers - Array of worker addresses to be retired
        """
        if workers is None:
            return

        closed_workers = await self.scheduler.retire_workers(workers=workers, retire=True, close_workers=True)
        self.clean_closed_workers(closed_workers)

    def clean_closed_workers(self, worker_addresses):
        """
            Clean up workers closed through reclaim

            Parameters:
            ----------
            worker_addresses: list(str)
                List of worker addresses to remove
        """
        for worker_address in worker_addresses:
            try:
                del self.worker_spec[worker_address]
            except KeyError:
                # Already removed
                pass

            try:
                del self.workers[worker_address]
            except KeyError:
                # Already removed
                pass

    async def clean_failed_workers(self):
        """
            Clean up workers that have failed too many times
        """
        worker_keys = []
        for w in self.workers:
            job = self.workers[w]
            if job.status == "failed":
                worker_keys.append(w)
                job.cancel_job()

        self.clean_closed_workers(worker_keys)

        if len(self.workers) == 0:
            self._unable_to_start_jobs_count += 1

        if self._unable_to_start_jobs_count == self._failed_job_limit:
            logger.error(_("The ConductorCluster is not able to start any jobs after %d attempts. "
                           "The cluster will close."), self._failed_job_limit)
            await self._close()

    @property
    def dashboard_link(self):
        """
            Override the Cluster class method to use FQDN instead of IP address for ConductorCluster
        """
        try:
            port = self.scheduler_info["services"]["dashboard"]
        except KeyError:
            return ""
        else:
            host = socket.getfqdn()
            return format_dashboard_link(host, port)


class ConductorPlugin(SchedulerPlugin):
    """
        Scheduler plugin for integration with IBM Spectrum Conductor
    """
    min_distributed_version = "2.5.2"  # Minimum supported distributed package version

    def __init__(self,
                 scheduler=None,
                 cluster=None,
                 manager_rest_address=None):

        if scheduler is None or cluster is None or manager_rest_address is None:
            raise ValueError(_("The ConductorPlugin must be initialized with scheduler, "
                               "cluster and manager_rest_address."))

        self.scheduler = scheduler
        self.cluster = cluster
        self.manager_rest_address = manager_rest_address

        self.removed_worker_addresses = []  # String of worker job/activity IDs that have been removed by the scheduler

        self.distributed_version = self._get_distributed_version()
        if version.parse(self.distributed_version) < version.parse(self.min_distributed_version):
            raise ValueError(_("The distributed version %s is not supported. "
                               "The ConductorPlugin requires distributed version %s or higher."),
                             self.distributed_version, self.min_distributed_version)

        self.scheduler.handlers.update({"reclaim-workers": self.reclaim_workers,
                                        "check-alive": self.check_alive,
                                        "activity-exit": self.activity_exit})

    def _get_distributed_version(self):
        """
            Get the version of the dask distributed package.
        """
        pkg_versions = versions.get_versions()
        try:
            required_packages = pkg_versions['packages']['required']
        except KeyError:
            # In Dask 2.9.2 the dictionary removes the 'required' tag
            return pkg_versions['packages']['distributed']

        for package in required_packages:
            package_name = package[0]
            if package_name == "distributed":
                return package[1]

    # These methods are called by the scheduler
    def add_worker(self, scheduler=None, worker=None, **kwargs):
        """
            Add a worker to the ConductorCluster
        """
        logger.debug("Add a Dask worker %s with name %s.", worker, scheduler.workers[worker].name)
        worker_name = scheduler.workers[worker].name
        self.cluster.update_launched_job(worker, worker_name)

    def remove_worker(self, scheduler=None, worker=None):
        """
            Remove a worker from the ConductorCluster
        """
        if worker in self.cluster.workers:
            job_id = self.cluster.workers[worker].job_id
            logger.debug("Remove the worker with job_id=%s", job_id)
            if job_id is not None and job_id != "NOT_STARTED":
                self.removed_worker_addresses.append(job_id)
            else:
                logger.warn(_("The worker with address %s does not have a job ID."), worker)

    # Handler methods
    async def reclaim_workers(self, comm, reclaim=None):
        """
            Method to handle reclaim requests from the dask manager service
        """
        # Close the comm first - don't need to send any response
        comm.close()

        reclaim_candidate_workers = self._reclaim_workers(reclaim)
        await self.cluster.retire_workers(workers=reclaim_candidate_workers)

    async def check_alive(self, comm):
        """
            Method to handle check alive request from the Dask manager service
        """
        await comm.write({"op": "alive"})
        comm.close()

    async def activity_exit(self, comm, activity=None):
        # Close the comm first - don't need to send any response
        comm.close()

        if activity is None:
            return

        activity_map = json.loads(activity)
        id = activity_map.get('id', None)

        if id is None:
            return

        state = activity_map.get('state', None)
        exit_code = activity_map.get('status', None)
        exit_msg = activity_map.get('reason', None)

        if state == "ERROR" or exit_code != "0":
            logger.warn(_("The worker with the activity ID %s did not exit successfully. Exit code: %s. Reason: %s."),
                        id, exit_code, exit_msg)

        if id in self.removed_worker_addresses:
            # Normal case - worker was closed first by the scheduler before the activity exited
            self.removed_worker_addresses.remove(id)
        else:
            running_workers = self.cluster.get_workers_with_job_id(id)
            if len(running_workers) == 1:
                logger.warn(_("The activity with the activity ID %s has exited, but the cluster thinks it is "
                              "still running. The worker's job will be cancelled."), id)
                running_workers.pop().cancel_job()
            elif len(running_workers) == 0:
                pending_jobs = self.cluster.get_workers_with_job_id()
                if len(pending_jobs) > 0:
                    logger.debug("Cancel one of the jobs that has not started as a result of failed worker activity.")
                    is_failed = pending_jobs.pop().fail_job(id)
                    if is_failed:
                        await self.cluster.clean_failed_workers()
                else:
                    # This will happen when the scheduler scales down its requested workers at the same time that EGO
                    # allocates a new worker. TThe resulting worker will not be added to the scheduler and the activity
                    # will be stopped when the activity is stopped.
                    # We get here because the worker was never added to the scheduler
                    logger.debug("The activity with ID %s has exited, but the scheduler does not have any worker "
                                 "with this ID or any pending workers.", id)
            else:
                logger.warn("Multiple workers matching activity ID %s were found.", id)

    def _reclaim_workers(self, reclaim=None):
        """
            Reclaim workers from the ConductorCluster

            Parameters:
            -----------
            reclaim: dict
                Dictionary of {hostname: num_workers} to be reclaimed
        """
        if reclaim is None:
            return

        reclaim_map = json.loads(reclaim)
        reclaim_candidate_workers = []
        for hostname in reclaim_map:
            num_workers = int(reclaim_map[hostname])
            reclaim_candidate_workers.extend(self._workers_to_reclaim(hostname, num_workers))

        return reclaim_candidate_workers

    def _workers_to_reclaim(self, hostname=None, num_jobs=0):
        """
            Suggest workers to reclaim on the specified host based on workers which have the least number of tasks
            in progress followed by the least amount of data in memory.

            Parameters:
            ----------
            hostname: string
                Name of the host to reclaim workers from

            num_jobs: int
                Number of jobs to be reclaimed

            Returns:
            --------
            List of worker addresses to be reclaimed
        """

        if num_jobs == 0:
            return []

        # Find the workers running on the specified host
        workers_on_host = list(filter(lambda ws: getattr(ws, 'name').startswith("Worker__" + hostname),
                                      self.scheduler.workers.values()))

        if(num_jobs >= len(workers_on_host)):
            logger.info(_("Reclaiming all Dask worker jobs on the host %s."), hostname)
            return workers_on_host

        workers_sorted_by_activity = sorted(workers_on_host, key=lambda ws:
                                            get_activity_id_from_worker_name(getattr(ws, "name")))

        # When ConductorCluster is configured with multiple processes or threads per Job(EGO activity), each may have
        # multiple workers associated. In the case of reclaim, we need to reclaim all workers for a single slot,
        # which maps to an activity. Aggregate the worker stats by activity ID so that we can determine which
        # overall activities are the least busy
        activities = []
        activity = {"id": None, "has_what": 0, "processing": 0, "addresses": []}
        for ws in workers_sorted_by_activity:
            ws_id = get_activity_id_from_worker_name(getattr(ws, "name"))
            if ws_id != activity["id"]:
                if activity["id"] is not None:
                    activities.append(activity)
                activity = {"id": ws_id, "has_what": 0, "processing": 0, "addresses": []}
            activity["has_what"] = activity["has_what"] + len(ws.has_what)
            activity["processing"] = activity["processing"] + len(ws.processing)
            activity["addresses"].append(ws.address)
        if activity["id"] is not None:
            activities.append(activity)

        # Sort activities by processing and has_what
        activites_sorted_by_workload = sorted(activities, key=lambda a: (a["has_what"], a["processing"]), reverse=False)
        logger.info(_("Reclaiming %d of the %d Dask worker jobs on the host %s."), num_jobs,
                    len(activites_sorted_by_workload), hostname)

        # Return all addresses for the activities
        addresses = []
        for a in activites_sorted_by_workload[0: num_jobs]:
            logger.debug("Reclaiming the EGO activity with ID %s, which has %d workers." % (
                a["id"] , len(a["addresses"])))
            addresses += a["addresses"]

        logger.info(_("As part of the reclaim, %d workers will be closed."), len(addresses))

        return addresses
