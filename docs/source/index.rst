Dask-Jobqueue
=============

*Easily deploy Dask on job queuing systems like PBS, Slurm, MOAB, SGE, LSF, and HTCondor.*


The ``dask-jobqueue`` project makes it easy to deploy Dask on common job queuing
systems typically found in high performance supercomputers, academic research
institutions, and other clusters.  

There are two common deployment patterns for Dask on HPC, **Dynamic Clusters** and **Batch Runners**, and ``dask-jobqueue`` has support for both.

Dynamic Clusters
----------------

A **Dynamic Cluster** is a Dask cluster where new workers can be added dynamically while the cluster is running.

In an HPC environment this generally means that the Dask Scheduler is run in the same location as the client code,
usually on a single compute node. Then workers for the Dask cluster are submitted as additional jobs to the job
queue scheduler.

This pattern works well on clusters where it is favourable to submit many small jobs.


.. code-block:: bash

   srun -n 1 dynamic_workload.py

.. code-block:: python

   # dynamic_workload.py
   from dask_jobqueue.slurm import SLURMCluster
   cluster = SLURMCluster()
   cluster.adapt(minimum=1, maximum=10)  # Tells Dask to call `srun -n 1 ...` when it needs new workers

   from dask.distributed import Client
   client = Client(cluster)  # Connect this local process to remote workers

   import dask.array as da
   x = ...  # Dask commands now use these distributed resources

**Benefits**

- Clusters can autoscale as a workload progresses.
- Small gaps in the HPC that would be otherwise unused can be backfilled.
- A workload can run slowly with a few workers during busy times and then scale up during quiet times.
- Workloads in intaractive environments can scale up and down as users run code manually.
- You don't need to wait for all nodes to be available before your workload starts, so jobs often start sooner.

To learn more see the `Dynamic Cluster documentation <clusters-overview.html>`_.

Batch Runners
-------------

A **Batch Runner** is a Dask cluster where the whole workload, including the client code, scheduler and workers
are submitted as a single allocation to the job queue scheduler. All of the processes within the workload coordinate
during startup and then work together to compute the Dask workload.

This pattern works well where large jobs are prioritised and node locality is important.

.. code-block:: bash

   srun -n 12 python batch_workload.py

.. code-block:: python

   # batch_workload.py
   from dask_jobqueue.slurm import SLURMRunner
   cluster = SLURMRunner()  # Boostraps all the processes into a client + scheduler + 10 workers

   # Only the client process will continue past this point

   from dask.distributed import Client
   client = Client(cluster)  # Connect this client process to remote workers

   import dask.array as da
   x = ...                   # Dask commands now use these distributed resources

**Benefits**

- Workers are generally colocated physically on the machine, so communication is faster, expecially with `UCX <https://blog.dask.org/2019/06/09/ucx-dgx>`_.
- Submitting many small jobs can be frowned upon on some HPCs, submitting a single large job is more typical of other HPC workloads. 
- All workers are guaranteed to be available when the job starts which can avoid oversubscribing workers.
- Clusters comprised of one large allocation tends to be more reliable than many small allocations.
- All processes have the same start and wall time.

To learn more see the `Batch Runner documentation <runners-overview.html>`_.

More details
------------
A good entry point to know more about how to use ``dask-jobqueue`` is
:ref:`talks-and-tutorials`.

.. toctree::
   :maxdepth: 1
   :caption: Getting Started

   install
   talks-and-tutorials

.. toctree::
   :maxdepth: 1
   :caption: Dynamic Clusters

   clusters-overview
   clusters-interactive
   clusters-howitworks
   clusters-configuration
   clusters-configuration-setup
   clusters-example-deployments
   clusters-configuration-examples
   clusters-advanced-tips-and-tricks
   clusters-api

.. toctree::
   :maxdepth: 1
   :caption: Batch Runners

   runners-overview
   runners-implementing-new
   runners-api

.. toctree::
   :maxdepth: 1
   :caption: Help & Reference

   debug
   changelog
   develop
   history


