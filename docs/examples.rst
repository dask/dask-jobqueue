Example Deployments
===================

Deploying dask-jobqueue on different clusters requires a bit of customization.
Below, we provide a few examples from real deployments in the wild:

Additional examples from other cluster welcome `here <https://github.com/dask/dask-jobqueue/issues/40>`_.

PBS Deployments
---------------

.. code-block:: python

   from dask_jobqueue import PBSCluster

   cluster = PBSCluster(queue='regular',
                        project='DaskOnPBS',
                        local_directory='$TMPDIR',
                        threads=4,
                        processes=6,
                        memory='16GB',
                        resource_spec='select=1:ncpus=24:mem=100GB')

   cluster = PBSCluster(processes=18,
                        threads=4,
                        memory="6GB",
                        project='P48500028',
                        queue='premium',
                        resource_spec='select=1:ncpus=36:mem=109G',
                        walltime='02:00:00',
                        interface='ib0')

Moab Deployments
~~~~~~~~~~~~~~~~

On systems which use the Moab Workload Manager, a subclass of ``PBSCluster``
can be used, called ``MoabCluster``:

.. code-block:: python

   import os
   from dask_jobqueue import MoabCluster

   cluster = MoabCluster(processes=6,
                         threads=1,
                         project='gfdl_m',
                         memory='16G',
                         resource_spec='pmem=96G',
                         job_extra=['-d /home/First.Last', '-M none'],
                         local_directory=os.getenv('TMPDIR', '/tmp'))

SGE Deployments
---------------

On systems which use SGE as the scheduler, ```SGECluster`` can be used:

.. code-block:: python

    from dask_jobqueue import SGECluster

    cluster = SGECluster(queue='default.q',
                         walltime="1500000",
                         processes=10,
                         memory='20GB')

LSF Deployments
---------------

.. code-block:: python

    from dask_jobqueue import LSFCluster

    cluster = LSFCluster(queue='general',
                         project='cpp'
                         walltime='00:30',
                         cores=15,
                         memory='25GB')

SLURM Deployments
-----------------

.. code-block:: python

   from dask_jobqueue import SLURMCluster

   cluster = SLURMCluster(processes=4,
                          threads=2,
                          memory="16GB",
                          project="woodshole",
                          walltime="01:00:00",
                          queue="normal")



SLURM Deployment: Low-priority node usage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python


    from dask_jobqueue import SLURMCluster

    cluster = SLURMCluster(processes=6,
                           threads=4,
                           memory="16GB",
                           project="co_laika",
                           queue='savio2_bigmem',
                           env_extra=['export LANG="en_US.utf8"',
                                      'export LANGUAGE="en_US.utf8"',
                                      'export LC_ALL="en_US.utf8"'],
                           job_extra=['--qos="savio_lowprio"'])


Viewing the Dask Dashboard
--------------------------

Sometimes the Dask Dashboard might not be directly accessible via the browser.
This may be due to the notebook being served at one IP address and the
Dashboard being served to another.
 
To solve this you can use SSH tunneling. 

If you are using a notebook it is recommended to follow the configuration instructions
given in the `Pangeo documentation <http://pangeo-data.org/setup_guides/hpc.html#configure-jupyter>`_.

Firstly, you can inspect the ``client`` to see the IP address it is being served to:

.. code-block:: python


    from dask_jobqueue import LSFCluster
    from distributed import Client

    cluster = LSFCluster(cores=2, memory='2GB')

    client = Client(cluster)

On inspection of the ``client`` object, you see that the client can be viewed at 
``172.16.23.102``, for example. It is then recommended the notebook is served at 
this IP address:

.. code-block:: bash

    
    $ jupyter notebook --no-browser --ip=172.16.23.102 --port=8888

This can be tunneled from a local machine as:

.. code-block:: bash

    
    $ ssh -N -L 8888:172.16.23.102:8888 USER@DOMAIN &

The notebook will now be accessible at ``http://localhost:8888`` on your browser.

To tunel the Dask Dashboard inspect the ``client`` object in the notebook and make 
note of the Dashboard port e.g. ``http://172.16.23.102:8787/status`` (it may not always 
be 8787). Lastly, tunel the Dashboard from a local machine as:

.. code-block:: bash

     
    $ ssh -N -L 8787:172.16.23.102:8787 USER@DOMAIN
