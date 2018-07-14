Configuration Examples
======================

We include configuration files for known supercomputers.
Hopefully these help both other users that use those machines and new users who
want to see examples for similar clusters.

Additional examples from other cluster welcome `here <https://github.com/dask/dask-jobqueue/issues/40>`_.

Cheyenne
--------

`Cheyenne Supercomputer <https://www2.cisl.ucar.edu/resources/computational-systems/cheyenne>`_

.. code-block:: yaml

   distributed:
     scheduler:
       bandwidth: 1000000000     # GB MB/s estimated worker-worker bandwidth
     worker:
       memory:
         target: 0.90  # Avoid spilling to disk
         spill: False  # Avoid spilling to disk
         pause: 0.80  # fraction at which we pause worker threads
         terminate: 0.95  # fraction at which we terminate the worker
     comm:
       compression: null

   jobqueue:
     pbs:
       cores: 36
       memory: 108GB
       processes: 4

       interface: ib0
       local-directory: $TMPDIR

       queue: regular
       project: null  # TODO, change me
       walltime: '00:30:00'

       resource-spec: select=1:ncpus=36:mem=109G

ARM Stratus
-----------

`Department of Energy Atmospheric Radiation Measurement (DOE-ARM) Stratus Supercomputer <https://adc.arm.gov/tutorials/cluster/stratusclusterquickstart.html>`_.

.. code-block:: yaml

    jobqueue:
      pbs:
        name: dask-worker
        cores: 36
        memory: 270GB
        processes: 6
        interface: ib0
        local-directory: $localscratch
        queue: high_mem # Can also select batch or gpu_ssd
        project: arm
        walltime: 00:30:00 #Adjust this to job size
        job-extra: ['-W group_list=cades-arm']
