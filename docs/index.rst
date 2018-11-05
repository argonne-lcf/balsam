.. balsam documentation master file, created by
   sphinx-quickstart on Fri Dec 15 13:22:13 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Balsam - HPC Workflow and Edge Service
======================================

Balsam is a Python service that automates scheduling and concurrent,
fault-tolerant execution of workflows in HPC environments. It is one of the
easiest ways to set up a large computational campaign, where many instances of
an application need to run across several days or weeks worth of batch jobs.
You use a command line interface or Python API to control a Balsam database, which
stores a **task** for each application instance. The Balsam **launcher** is then
started inside a batch job to actually run the available work.  The launcher
automatically consumes tasks from the database, runs them in parallel across the available compute
nodes, and records workflow state in the database. 

The persistence of workflow
state in the database is what allows Balsam to capture and record task level
errors, auto-restart timed out tasks, and schedule batch jobs over time. The
underlying PostgreSQL database server is a core part of the Balsam technology,
but database administration tasks and SQL in general are completely hidden from
users and encapsulated in Balsam.

It also gives users flexible
tools to query the status of their campaign, and

Balsam automatically 1
many instances of some application need to run across  
It runs on the login nodes, keeping
track of all your jobs and submitting them to the local scheduler on your
behalf.

Whereas a local batch scheduler like Cobalt runs on behalf of **all users**,
with the goals of fair resource sharing and maximizing overall utilization,
Balsam runs on **your** behalf, interacting with the scheduler to check for
idle resources and sizing jobs to minimize time-to-solution.

You could use Balsam as a drop-in replacement for ``qsub``, simply using
``balsam qsub`` to submit your jobs with absolutely no restrictions. Let Balsam
throttle submission to the local queues, package jobs into ensembles for you,
and dynamically size these packages to exploit local scheduling policies.

There is :doc:`much more <userguide/overview>` to Balsam, which is a complete
service for managing complex workflows and optimized scheduling across multiple
HPC resources.


.. toctree::
    :maxdepth: 2
    :caption: Quickstart

    quick/quickstart.rst
    quick/db.rst
    quick/hello.rst

.. toctree::
    :maxdepth: 2
    :caption: User Guide

    userguide/overview
    userguide/tutorial-theta.rst
    userguide/tutorial.rst
    userguide/dag
    userguide/multi-user

.. _dev_docs:
.. toctree::
    :maxdepth: 3
    :caption: Developer Documentation

    devguide/roadmap
    devguide/launcher

.. toctree::
    :maxdepth: 2
    :caption: Use Cases

    example/dl-hps.rst
    example/recipes.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
