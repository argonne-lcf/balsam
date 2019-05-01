.. balsam documentation master file, created by
   sphinx-quickstart on Fri Dec 15 13:22:13 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


Balsam: HPC Workflows & Edge Service
=========================================

Balsam allows you to manage a database of workflows with simple command-line
interfaces and a Python API. As you populate the database, the **Balsam
service** can automatically reserve bundles of tasks and batch-schedule them
for parallel execution. Inside each batch job, the **Balsam launcher**
monitors available resources, launches applications, and sends status updates
back to the database.

Balsam is designed to minimize user "buy-in" and cognitive overhead.
You don't have to learn an API or write any glue code to acheive throughput with
existing applications. In fact, it's arguably faster and easier to run a
simple app several times using Balsam than by writing an ensemble job script:

.. highlight:: console

::

    $ balsam app --name SayHello --executable "echo hello,"
    $ for i in {1..10}
    > do
    >  balsam job --name hi$i --workflow test --application SayHello --args "world $i"
    > done
    $ balsam submit-launch -A Project -q Queue -t 5 -n 2 --job-mode=serial
    ```

Highlights
----------------

- Applications require zero modification and run *as-is* with Balsam
- Launch MPI applications or pack several non-MPI tasks-per-node
- Run apps on bare metal or inside a Singularity_ container 
- Flexible Python API and command-line interfaces for workflow management
- Execution is load balanced and resilient to task faults. Errors are automatically recorded to database for quick lookup and
  debugging of workflows
- Scheduled jobs can overlap in time; launchers cooperatively consume work from the same database
- Multi-user workflow management: collaborators on the same project can add tasks and submit launcher jobs using
  the same database

The Balsam API enables a variety of scenarios beyond the independent bag-of-tasks:

- Add task dependencies to form DAGs
- Program dynamic workflows: some tasks spawn or kill other tasks at runtime
- Remotely submit workflows, track their progress, and coordinate data movement tasks

.. toctree::
    :maxdepth: 2
    :caption: User Guide

    userguide/getting-started.rst
    userguide/configuration.rst
    userguide/db.rst
    userguide/app.rst
    userguide/submit.rst
    userguide/cli.rst
    userguide/dag.rst
    userguide/multi-user.rst

.. toctree::
    :maxdepth: 2
    :caption: Tutorials

    tutorial/hello.rst
    tutorial/deephyper.rst
    tutorial/tutorial-theta.rst

.. toctree::
    :maxdepth: 2
    :caption: FAQs & Common Recipes

    faq/dl-hps.rst
    faq/recipes.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _Singularity: https://www.alcf.anl.gov/user-guides/singularity
