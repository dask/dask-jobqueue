Overview
========

The batch runner classes are designed to make it simple to write Python scripts that will leverage multi-node jobs in an HPC.

For example if we write a Python script for a Slurm based system and call it with ``srun -n 6 python myscript.py`` the script will be invoked by Slurm 
6 times in parallel on 6 different nodes/cores on the HPC. The Dask Runner class then uses the Slurm process ID environment 
variable to decide what role reach process should play and uses the shared filesystem to bootstrap communications with a scheduler file.

.. code-block:: python

    # myscript.py
    from dask.distributed import Client
    from dask_jobqueue.slurm import SlurmRunner

    # When entering the SlurmRunner context manager processes will decide if they should be 
    # the client, schdeduler or a worker.
    # Only process ID 1 executes the contents of the context manager.
    # All other processes start the Dask components and then block here forever.
    with SlurmRunner(scheduler_file="/path/to/shared/filesystem/scheduler-{job_id}.json") as runner:

        # The runner object contains the scheduler address info and can be used to construct a client.
        with Client(runner) as client:

            # Wait for all the workers to be ready before continuing.
            client.wait_for_workers(runner.n_workers)

            # Then we can submit some work to the Dask scheduler.
            assert client.submit(lambda x: x + 1, 10).result() == 11
            assert client.submit(lambda x: x + 1, 20, workers=2).result() == 21

    # When process ID 1 exits the SlurmRunner context manager it sends a graceful shutdown to the Dask processes.
