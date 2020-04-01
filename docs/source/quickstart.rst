.. _quickstart:

QuickStart
==============

Here is a VERY simple example of how Dask works and distributes tasks on the HPC.

In order to really understand how this works have one terminal open on a login node to your HPC with ipython or jupyterhub, and another that is running `watch squeue`.

.. code-block:: python
    from dask.distributed import Client
    from dask_jobqueue import SLURMCluster
    cluster = SLURMCluster(cores=1, processes=1, memory="1GB")
    client = Client(cluster)

If you check your other terminal you will see that there are no HPC jobs running. You have to tell the cluster to scale up some jobs. When you run `cluster.scale(jobs=N)` you will scale to that number of HPC jobs.

.. code-block:: python
    cluster.scale(jobs=1, memory="1GB")

Now check your terminal that has `watch squeue` and you should see some jobs either running or pending. You can get the typical SLURM job information by running `scontrol show job $JOB_ID` with the job_id from squeue.

If you run `cluster.scale(jobs=1)` there will be no change, because you are scaling to 1 job. If you need more jobs you'll have to increase the number of jobs available to the Dask Jobqueue.

.. code-block:: bash
    [ec2-user@ip-172-31-108-237 ~]$ scontrol show job 34
    JobId=34 JobName=dask-worker
       UserId=ec2-user(1000) GroupId=ec2-user(1000) MCS_label=N/A
       Priority=4294901731 Nice=0 Account=(null) QOS=(null)
       JobState=PENDING Reason=Nodes_required_for_job_are_DOWN,_DRAINED_or_reserved_for_jobs_in_higher_priority_partitions Dependency=(null)
       Requeue=1 Restarts=0 BatchFlag=1 Reboot=0 ExitCode=0:0
       RunTime=00:00:00 TimeLimit=00:30:00 TimeMin=N/A
       SubmitTime=2020-04-01T07:37:31 EligibleTime=2020-04-01T07:37:31
       AccrueTime=2020-04-01T07:37:31
       StartTime=Unknown EndTime=Unknown Deadline=N/A
       SuspendTime=None SecsPreSuspend=0 LastSchedEval=2020-04-01T07:39:16
       Partition=compute AllocNode:Sid=ip-172-31-108-237:6440
       ReqNodeList=(null) ExcNodeList=(null)
       NodeList=(null)
       NumNodes=1 NumCPUs=1 NumTasks=1 CPUs/Task=1 ReqB:S:C:T=0:0:*:*
       TRES=cpu=1,mem=954M,node=1,billing=1
       Socks/Node=* NtasksPerN:B:S:C=0:0:*:* CoreSpec=*
       MinCPUsNode=1 MinMemoryNode=954M MinTmpDiskNode=0
       Features=(null) DelayBoot=00:00:00
       OverSubscribe=OK Contiguous=0 Licenses=(null) Network=(null)
       Command=/tmp/tmpe30sufqh.sh
       WorkDir=/home/ec2-user
       StdErr=/home/ec2-user/slurm-34.out
       StdIn=/dev/null
       StdOut=/home/ec2-user/slurm-34.out
       Power=

If you take a look at the `StdOut` file you'll see that there is a some helpful debug info from the Dask Worker.

.. code-block:: bash
    distributed.nanny - INFO -         Start Nanny at: 'tcp://172.31.96.187:33219'
    distributed.dashboard.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip
     install jupyter-server-proxy
    distributed.worker - INFO -       Start worker at:  tcp://172.31.96.187:42375
    distributed.worker - INFO -          Listening to:  tcp://172.31.96.187:42375
    distributed.worker - INFO -          dashboard at:        172.31.96.187:36127
    distributed.worker - INFO - Waiting to connect to: tcp://172.31.108.237:42811
    distributed.worker - INFO - -------------------------------------------------
    distributed.worker - INFO -               Threads:                          1
    distributed.worker - INFO -                Memory:                 1000.00 MB
    distributed.worker - INFO -       Local Directory: /home/ec2-user/dask-worker-space/worker-pwncsx0p
    distributed.worker - INFO - -------------------------------------------------
    distributed.worker - INFO -         Registered to: tcp://172.31.108.237:42811
    distributed.worker - INFO - -------------------------------------------------
    distributed.core - INFO - Starting established connection


Let's write out some information about our jobs to a file. This will help you to understand how Dask is passing tasks off to your HPC jobs. If you are using a scheduler besides SLURM you will need to change the environmental variables to have this be meaningful.

.. code-block:: python
    import os
    from pathlib import Path

    # Make sure your file is somewhere with mounted storage across all nodes
    home = str(Path.home())
    file_name = os.path.join(home, 'dask-jobqueue-info.txt')

    def print_env(x):
        env_vars = ['HOSTNAME', 'SLURM_JOB_ID', 'SLURM_JOB_NAME', 'SLURM_NODEID']
        with open(file_name, "a") as myfile:
            myfile.write("#############################\n")
            myfile.write('Print Env Id: {}'.format(x))
            for e in env_vars:
                myfile.write("{}: {}\n".format(e, os.environ.get(e)))
            myfile.write("#############################\n")
        return x

    futures = client.map(print_env, range(10))
    results = client.gather(futures)
    len(results)
    # Should be 100

There's no actual point to this function. It is to demonstrate a few concepts that are important to both Dask and DaskJobqueue.

Let's take a look at the `~/dask-jobqueue-info.txt` file. First of all you can just grep for the 'Print' to demonstrate that this did indeed run 100 times with:

.. code-block:: bash
    cat ~/dask-jobqueue-info.txt |grep Print |wc -l

If you want to know how your jobs would be distributed among multiple jobs scale again.

.. code-block:: python
    cluster.scale(jobs=3, memory="1GB")

As a quick side note, Dask is smart and serializes your task. It knows you already did this, and so if you're testing and are trying to figure out why something isn't running check out the futures object and see if its finished or not. You'll have to change the input arguments to get it to rerun.

.. code-block:: python
    # Take a look at the futures object
    print(futures[0])
    #  <Future: finished, type: builtins.int, key: print_env-1c617e0e3fc9f0203f82ce3694fd37d5>
    futures = client.map(print_env, range(100, 200))
    results = client.gather(futures)
    len(results)

.. code-block:: bash
    cat ~/dask-jobqueue-info.txt |grep SLURM_JOB_ID

You should see something like this repeated for the amount of tasks you submitted with Dask.

.. code-block:: bash
    SLURM_JOB_ID: 38
    SLURM_JOB_ID: 37
    SLURM_JOB_ID: 38
    SLURM_JOB_ID: 39
    SLURM_JOB_ID: 37

