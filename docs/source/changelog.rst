Changelog
=========

Development version
-------------------

0.9.0 / 2024-08-22
------------------

- Add SLURMRunner from jacobtomlinson/dask-hpc-runners (:pr:`659`)
- Bump minimum Python to 3.10 (:pr:`662`)
- Fixed LSFCluster stdin job setup not being run in a shell (:pr:`661`)
- Remove unused lsf_version function for Python 3.12 and fix never awaited error for LSFCluster (:pr:`646`)
- Handle outdated root certificates (:pr:`651`)
- Migrate Slurm tests to use rockylinux (:pr:`650`)
- Migrate PBS tests to rocklinux 8 and openpbs 23.06 (:pr:`649`)
- Fix bug in OARJob where Job._call is not awaited (:pr:`642`)

Thanks to `@aiudirog <https://github.com/aiudirog>`_, `@tcztzy <https://github.com/tcztzy>`_ and `@sjdv1982 <https://github.com/sjdv1982>`_.

0.8.5 / 2024-02-22
------------------

- Update versioneer for 3.12 compatibility (:pr:`618`)
- Make cli worker parameter flexible (:pr:`606`)
- Asynchronous job submission and removal (:pr:`610`)
- Python executable from config file (:pr:`623`)
- Small doc fixes and housekeeping (:pr:`621`, :pr:`625`)

Thanks to `@hmacdope <https://github.com/hmacdope>`_, `@jrueb <https://github.com/jrueb>`_, 
`@Andrew-S-Rosen <https://github.com/Andrew-S-Rosen>`_, `@fnattino <https://github.com/fnattino>`_,
`@eckhrd <https://github.com/eckhrd>`_, `@cbouss <https://github.com/cbouss>`_ and 
`@jacobtomlinson <https://github.com/jacobtomlinson>`_.


0.8.2 / 2023-06-15
------------------

- Extend OARCluster implementation to let OAR take into account the memory parameter (:pr:`598`, :pr:`595`)

0.8.1 / 2022-10-04
------------------

- Fix the multiple ``--interface`` CLI argument bug (:pr:`591`)
- Change ``project`` to ``account`` where appropriate (PBS, Slurm) (:pr:`586`)
- Do not skip ``job_extra_directives`` with ``header_skip`` values and rename ``header_skip`` to ``job_directives_skip`` (:pr:`584`)
- Various CI updates, HTCondor Docker build (:pr:`588`, :pr:`587`, :pr:`583`, :pr:`582`, :pr:`581`, :pr:`580`)

Thanks to `@jolange <https://github.com/jolange>`_ and `@guillaumeeb <https://github.com/guillaumeeb>`_.

0.8.0 / 2022-08-29
------------------

- Use --nworkers instead of deprecated --nprocs in the generated job scripts (:pr:`560`)
- Drop support for Python 3.7 (:pr:`562`)
- Rename ``env_extra`` kwarg to ``job_script_prologue`` (:pr:`575`)
- Rename ``extra`` kwarg to ``worker_extra_args`` (:pr:`576`)
- Rename ``job_extra`` kwarg to ``job_extra_directives`` (:pr:`577`)
- Fixing CI failures (:pr:`562`, :pr:`574`)
- Fix behaviour of env_extra for HTCondor and other related fixes (:pr:`563`, :pr:`570`, :pr:`572`)
- Add batch_name to match the name of the Dask worker in HTCondor (:pr:`571`)

Thanks to `@jolange <https://github.com/jolange>`_, `@ikabadzhov <https://github.com/ikabadzhov>`_ and `@guillaumeeb <https://github.com/guillaumeeb>`_.

0.7.4 / 2022-07-13
------------------

- Testing fixes (:pr:`538`, :pr:`537`, :pr:`533`, :pr:`532`, :pr:`531`, :pr:`523`, :pr:`511`)
- Drop support for Python 3.6 (:pr:`279`)
- Fix docstring as HTCondor needs no shared filesystem (:pr:`536`)
- Fix some utils deprecations (:pr:`529`)
- Add the possibility to use TLS and auto generate certificates (:pr:`519`, :pr:`524`, :pr:`526`, :pr:`527`)
- Adding extra argument to condor_submit (:pr:`411`)

0.7.3 / 2021-07-22
------------------

- Override _new_worker_name to make it easier to use job arrays (:pr:`480`)
- Drop support for Python 3.5 (:pr:`456`)
- Remove `FutureWarning`s from dask utils functions. (:pr:`503` and :pr:`506`)

0.7.2 / 2020-12-07
------------------

- Use ``Status`` enum (:pr:`476`)
- Bump GHA ``setup-miniconda`` version (:pr:`474`)
- Build docker images for scheduled runs (:pr:`468`)
- Blacken after likely a black change
- Add GH action to periodically build docker images (:pr:`455`)
- Fix link format inside an italicised text (:pr:`460`)
- ``MoabCluster``: fix bug where ``MoabCluster`` was using the ``jobqueue.pbs``
  config section rather than the ``jobqueue.moab`` section (:pr:`450`)
- Updating ``start_workers`` to scale in examples (:pr:`453`)
- Fixing typo in ``core.py`` (:pr:`454`)
- Improve doc about GiB vs GB
- Fix math typo in GiB definition (:pr:`445`)
- Improve doc about customising dashboard link
- Remove Travis mentions following Github Actions switch (:pr:`444`)
- Improve error message.
- Tweak name in ``cluster.job_script()`` (:pr:`439`)
- Switch from Travis to GitHub Actions (:pr:`435`)
- All cluster classes: fix a bug that would allow to pass any named parameter
  without an error (:pr:`398`)
- Use pre-built docker images to speed up CI (:pr:`432`)
- Rename common work-arounds section.
- Kick-off doc section about common work-arounds (:pr:`430`)
- Clean up parametrized tests (:pr:`429`)
- All cluster classes: ``scheduler_options`` parameter can be set through the
  config file in the ``scheduler-options`` section (:pr:`405`)
- Add minimal HTCondor CI support (:pr:`420`)
- Add content about the python executable used by workers in SLURM (:pr:`409`)
- Remove ``config_name`` from cluster classes (:pr:`426`)
- Fix mysql version to get Slurm CI green (:pr:`423`)
- Fix URL for miniconda download (:pr:`412`)


0.7.1 / 2020-03-26
------------------

- all cluster classes: add ``scheduler_options`` allows to pass parameters to
  the Dask scheduler. For example ``scheduler_options={'interface': 'eth0',
  dashboard_addresses=':12435')`` (:pr:`384`). Breaking change: using ``port``
  or ``dashboard_addresses`` arguments raises an error. They have to be passed
  through ``scheduler_options``.
- all cluster classes: ``processes`` parameter default has changed. By default,
  ``processes ~= sqrt(cores)`` so that the number of processes and the number
  of threads per process is roughly the same. Old default was to use one
  process and only threads, i.e. ``proccesses=1``,
  ``threads_per_process=cores``. (:pr:`375`)
- all cluster classes: ``interface`` was ignored when set in a config file.
  (:pr:`366`)
- ``LSFCluster``: switch to ``use_stdin=True`` by default (:pr:`388`).
- ``LSFCluster``: add ``use_stdin`` to ``LSFCluster``. This switches between
  ``bsub < job_script`` and ``bsub job_script`` to launch a ``LSF`` job
  (:pr:`360`).
- ``HTCondorCluster``: support older ``HTCondor`` versions without ``-file``
  argument (:pr:`351`).
- ``OARCluster``: fix bug (forgotten async def) in ``OARCluster._submit_job`` (:pr:`380`).

0.7.0 / 2019-10-09
------------------

- Base Dask-Jobqueue on top of the core ``dask.distributed.SpecCluster`` class
  (:pr:`307`)

  This is nearly complete reimplementation of the dask-jobqueue logic on top
  of more centralized logic.  This improves standardization and adds new
  features, but does include the following **breaking changes**:

  + The ``cluster.start_workers`` method has been removed. Use
    ``cluster.scale`` instead.
  + The ``cluster.stop_all_jobs()`` method has been removed.
    Please use ``cluster.scale(0)`` instead.
  + The attributes ``running_jobs``, ``pending_jobs``, and
    ``cancelled_jobs`` have been removed.  These have been moved upstream to
    the ``dask.distributed.SpecCluster`` class instead as ``workers`` and
    ``worker_spec``, as well as ``.plan``, ``.requested``, and ``.observed``.
  + The ``name`` attribute has been moved to ``job_name``.
- You can now specify jobs in ``.scale`` and ``.adapt``: for example
  ``cluster.scale(jobs=2)`` and ``cluster.adapt(minimum_jobs=0,
  maximum_jobs=10)``. Specifying scaling in terms of jobs is generally more
  intuitive than in terms of Dask workers. This was part of :pr:`307`.
- Update ``.scale()`` and ``.adapt()`` docstrings (:pr:`346`)
- Update interactive docs (:pr:`340`)
- Improve error message when cores or memory is not specified (:pr:`331`)
- Fix Python 3.5.0 support in setup.py (:pr:`317`)


0.6.3 / 2019-08-18
------------------

- Compatibility with Dask 2.3.0: add scheduler_info from
  local_cluster (:pr:`313`)
- Remove lingering Python 2 specific code (:pr:`308`)
- Remove __future__ imports since we depend on Python >3.5 (:pr:`311`)
- Remove Python 3 check for black in CI (:pr:`315`)

0.6.2 / 2019-07-31
------------------

- Ensure compatibility with Dask 2.2 (:pr:`303`)
- Update documentation

0.6.1 / 2019-07-25
------------------

- more fixes related to ``distributed >= 2`` changes (:pr:`278`, :pr:`291`)
- ``distributed >= 2.1`` is now required (:pr:`295`)
- remove deprecated ``threads`` parameter from all the ``Cluster`` classes (:pr:`297`)
- doc improvements (:pr:`290`, :pr:`294`, :pr:`296`)

0.6.0 / 2019-07-06
------------------

- Drop Python 2 support (:pr:`284`)
- Fix adaptive compatibility with SpecificationCluster in Distributed 2.0 (:pr:`282`)

0.5.0 / 2019-06-20
------------------

- Keeping up to date with Dask and Distributed (:pr:`268`)
- Formatting with Black (:pr:`256`, :pr:`248`)
- Improve some batch scheduler integration (:pr:`274`, :pr:`256`, :pr:`232`)
- Add HTCondor compatibility (:pr:`245`)
- Add the possibility to specify named configuration (:pr: `204`)
- Allow free configuration of Dask diagnostic_port (:pr: `192)`
- Start work on ClusterManager, see https://github.com/dask/distributed/issues/2235 (:pr:`187`, :pr:`184`, :pr:`183`)
- A lot of other tiny fixes and improvements(:pr:`277`, :pr:`261`, :pr:`260`, :pr:`250`, :pr:`244`, :pr:`200`, :pr:`189`)

0.4.1 / 2018-10-18
------------------

- Handle worker restart with clearer message (:pr:`138`)
- Better error handling on job submission failure (:pr:`146`)
- Fixed Python 2.7 error when starting workers (:pr:`155`)
- Better handling of extra scheduler options (:pr:`160`)
- Correct testing of Python 2.7 compatibility (:pr:`154`)
- Add ability to override python used to start workers (:pr:`167`)
- Internal improvements and edge cases handling (:pr:`97`)
- Possibility to specify a folder to store every job logs file (:pr:`145`)
- Require all cores on the same node for LSF (:pr:`177`)

0.4.0 / 2018-09-06
------------------

- Use number of worker processes as an argument to ``scale`` instead of
  number of jobs.
- Bind scheduler bokeh UI to every network interfaces by default.
- Adds an OAR job queue system implementation.
- Adds an LSF job queue system implementation.
- Adds some convenient methods to JobQueueCluster objects: ``__repr__``,
  ``stop_jobs()``, ``close()``.
