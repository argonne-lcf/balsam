.. balsam documentation master file, created by
   sphinx-quickstart on Fri Dec 15 13:22:13 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Balsam - HPC Workflow and Edge Service
======================================

Balsam is a Python-based service that handles the cumbersome process of running
many jobs across one or more HPC resources. It runs on the login nodes, keeping
track of all your jobs and submitting them to the local scheduler on your
behalf.

Why do I want this?
--------------------
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
