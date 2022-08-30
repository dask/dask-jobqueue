Development Guidelines
======================

This repository is part of the Dask_ projects.  General development guidelines
including where to ask for help, a layout of repositories, testing practices,
and documentation and style standards are available at the `Dask developer
guidelines`_ in the main documentation.

.. _Dask: https://dask.org
.. _`Dask developer guidelines`: https://docs.dask.org/en/latest/develop.html

Install
-------

After setting up an environment as described in the `Dask developer
guidelines`_ you can clone this repository with git::

   git clone git@github.com:dask/dask-jobqueue.git

and install it from source::

   cd dask-jobqueue
   python setup.py install

Formatting
----------

When you’re done making changes, check that your changes pass flake8 checks and use black formatting::

   flake8 dask_jobqueue
   black dask_jobqueue

To get flake8 and black, just pip install them. You can also use pre-commit to add them as pre-commit hooks.

Test without a Job scheduler
----------------------------

Test using ``pytest``::

   pytest dask_jobqueue --verbose

Test with a dockerized Job scheduler
------------------------------------

Some tests require to have a fully functional job queue cluster running, this
is done through Docker_ and `Docker compose`_ tools. You must thus have them
installed on your system following their docs.

You can then use the same commands as the CI for running the tests locally.
For example for PBS you can run::

   source ci/pbs.sh
   jobqueue_before_install
   jobqueue_install
   jobqueue_script

.. _Docker: https://www.docker.com/
.. _`Docker compose`: https://docs.docker.com/compose/

Testing without CI scripts
--------------------------

You can also manually launch tests with dockerized jobs schedulers (without CI commands), 
for a better understanding of what is going on.
This is basically a simplified version of what is in the ci/*.sh files.

For example with Slurm::

   cd ci/slurm
   docker-compose pull
   # Start a Slurm dockerized cluster
   ./start-slurm.sh #which is doing docker-compose up -d --no-build
   # Install dask-jobqueue in Docker container
   docker exec slurmctld /bin/bash -c "cd /dask-jobqueue; pip install -e ."
   # Run the tests for slurm
   docker exec slurmctld /bin/bash -c "pytest /dask-jobqueue/dask_jobqueue --verbose -E slurm -s"

You can then shutdown the dockerized cluster and remove all the containers from your computer::

   docker-compose down

Test on a real Job queuing system
---------------------------------

If you have installed dask-jobqueue on an HPC Center with a working Job Scheduler, 
you can also launch the tests requiring one from there.
Those are the tests with the @pytest.mark.env("scheduler-name") pytest fixture before the function.

With a cluster "cluster-name" (which needs to match the name in the pytest.mark.env)::

   pytest dask_jobqueue -E <cluster-name>

So for example with a Slurm cluster::

   pytest dask_jobqueue -E slurm

Note that this last feature has not been thoroughly tested, and you might run into timeout 
issues or other unexpected failures depending on your Job Scheduler configuration and load.

Building the Docker Images
--------------------------

Under the hood, the CI commands use or build Docker images.
You can also build these Docker images on your local computer if you need to update them.

For Slurm for example::

   cd ci/slurm
   cp ../environment.yml environment.yml #The Dockerfile needs the reference Conda environment file in its context to build
   docker-compose build

You might want to stop your dockerized cluster and refresh the build if you have done this previously::

   docker-compose down
   docker-compose build --no-cache

Update Docker images for CI tests
---------------------------------

The CI build testing dask-jobqueue on various dockerized Job Schedulers (.github/workflows/ci.yaml)
does not build the Docker images on each run and only pull them from Dockerhub.
It relies on another workflow (build-docker-images.yaml) to build and push these images.
This allows to speed-up CI checks.

Sometimes, (e.g. usually when updating Python version for tests), we will need to both update
the Docker images and modify and run tests accordingly. This should be done with two different PRs:

- One updating the Docker images. Tests might fail on this first one.
- Another to check tests using the newly built Docker images, once the first one is merged and the
  build-docker-images workflow has terminated.
