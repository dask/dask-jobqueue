.. _faq:

Frequently Asked Questions
==========================

What is the difference between a Job and a Worker?
--------------------------------------------------

In dask-distributed, a ``Worker`` is a Python object and node in a dask
``Cluster`` that serves two purposes, 1) serve data, and 2) perform\
computations. Jobs are resources submitted to, and managed by, the job queueing
system (e.g. PBS, SGE, etc.). In dask-jobqueue, a single Job may include one or
more Workers.
