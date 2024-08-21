Writing your own runners
========================

This document describes the design of the runners class and how to implement your own Dask runners.

The core assumption in the design of the runner model is that the same script will be executed many times by a job scheduler. 

.. code-block:: text

    (mpirun|srun|qsub|etc) -n4 myscript.py
    ├── [0] myscript.py
    ├── [1] myscript.py
    ├── [2] myscript.py
    └── [3] myscript.py

Within the script the runner class is created early on in the execution. 

.. code-block:: python

    from dask_jobqueue import SomeRunner
    from dask.distributed import Client

    with SomeRunner(**kwargs) as runner:
        with Client(runner) as client:
            client.wait_for_workers(2)
            # Do some Dask work

This will result in multiple processes runnning on an HPC that are all instantiating the runner class. 

The processes need to coordinate to decide which process should run the Dask Scheduler, which should be Dask Workers 
and which should continue running the rest of the client code within the script. This coordination happens during the 
``__init__()`` of the runner class.

The Scheduler and Worker processes exit after they complete to avoid running the client code multiple times. 
This means that only one of the processes will continue past the ``__init__()`` of the runner class, the rest will 
exit at that point after the work is done.

Base class
----------

In ``dask_jobqueue.runners`` there is a ``BaseRunner`` class that can be used for implementing other runners. 

The minimum required to implement a new runner is the following methods.

.. code-block:: python

    from dask_jobqueue.runner import BaseRunner

    class MyRunner(BaseRunner):

        async def get_role(self) -> str:
            """Figure out whether I am a scheduler, worker or client.

            A common way to do this is by using a process ID. Many job queues give each process
            a monotonic index that starts from zero. So we can assume proc 0 is the scheduler, proc 1
            is the client and any other procs are workers.
            """
            ...

        async def get_scheduler_address(self) -> str:
            """If I am not the scheduler discover the scheduler address.

            A common way to do this is to read a scheduler file from a shared filesystem.

            Alternatively if the scheduler process can broadcast it's address via something like MPI
            we can define ``BaseRunner.set_scheduler_address()`` which will be called on the scheduler 
            and then recieve the broadcast in this method.
            """
            ...

The ``BaseRunner`` class handles starting up Dask once these methods have been implemented. 
It also provides many stubbed out hooks to allow you to write code that runs before/after each component is created. 
E.g ``BaseRunner.before_scheduler_start()``, ``BaseRunner.before_worker_start()`` and ``BaseRunner.before_client_start()``.

The runner must know the address of the scheduler so that it can coordinate the clean shutdown of all processes when we 
reach the end of the code (either via ``__exit__()`` or a finalizer). This communication happens independently of 
any clients that may be created.

Slurm implementation example
----------------------------

As a concrete example you can look at the Slurm implementation.

In the ``get_role()`` method we use the ``SLURM_PROCID`` environment variable to infer the role.

We also add a default scheduler option to set the ``scheduler_file="scheduler-{job_id}.json"`` and I look up the 
Job ID from the ``SLURM_JOB_ID`` environment variable to ensource uniqueness. This effectively allows us to broadcast 
the scheduler address via the shared filesystem.

Then in the ``get_scheduler_address()`` method we wait for the scheduler file to exist and then open and read the 
address from the scheduler file in the same way the ``dask.distributed.Client`` does. 

Here's a cut down example for demonstration purposes.


.. code-block:: python

    from dask_jobqueue.runner import BaseRunner

    class SLURMRunner(BaseRunner):
        def __init__(self, *args, scheduler_file="scheduler.json", **kwargs):
            # Get the current process ID from the environment
            self.proc_id = int(os.environ["SLURM_PROCID"])

            # Tell the scheduler and workers to use a scheduler file on the shared filesystem
            self.scheduler_file = scheduler_file
            options = {"scheduler_file": self.scheduler_file}
            super().__init__(*args, worker_options=options, scheduler_options=options)

        async def get_role(self) -> str:
            # Choose the role for this process based on the process ID 
            if self.proc_id == 0 and self.scheduler:
                return Role.scheduler
            elif self.proc_id == 1 and self.client:
                return Role.client
            else:
                return Role.worker

        async def get_scheduler_address(self) -> str:
            # Wait for the scheduler file to be created and read the address from it
            while not self.scheduler_file or not self.scheduler_file.exists():
                await asyncio.sleep(0.2)
            cfg = json.loads(self.scheduler_file.read_text())
            return cfg["address"]
