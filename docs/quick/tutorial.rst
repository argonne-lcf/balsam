Balsam Tutorial
===================

The goal of Balsam is to manage and optimize workflow execution with a minimum of user intervention,
while placing very few restrictions on what kinds of jobs can run. Because of this, it takes some 
planning to decompose your workflow into BalsamJobs and write appropriate processing scripts to move
everything along.

To illustrate how a dynamic workflow might be designed, let's walk through a
simple example. A parent job (**A**) randomly generates between 2 and 6
symmetric matrices in "output files" that might represent the output of some
simulation. We would like to get the eigenvalues of all these matrices, in
sorted order.  We dynamically generate child jobs (**B**, **C**, **D**, ...)
for each of the matrix files, and these jobs find the eigenvalues of their
respective matrices.  Finally, the eigenvalues are collected and summarized by
job **R**, which sorts the eigenvalues. This dummy workflow can be represented
by the directed acyclic graph (with all edges pointing down)::
    
            A           # generate matrices
         /  |  \
        B   C   D       # eigendecompositions
         \  |  /
            R           # reduction job

Defining Applications
----------------------

A BalsamJob simply wraps one single execution of an application.  In order to help Balsam
build performance models of our applications and facilitate multi-site execution, we **define** 
each application in our workflow through an ``ApplicationDefinition`` entry in the Balsam database.
You can think of each ``ApplicationDefinition`` as an alias for the application, along with the following
fields:

    1. **name**
    2. description
    3. **executable path**
    4. preprocessing script
    5. postprocessing script
    6. enviroment variables

Only the **bold** fields are required; everything else is optional and can be overriden by specific BalsamJobs that 
call those applications. First, let's create the applications for our workflow. The applications can point to any kind of 
executable.  If it's an interpreted script, you can:

    * specify the interpreter at the top of the script and add execute permissions to the script itself
    * specify both the path of the interpreter and the script (e.g. ``/bin/bash /path/to/my/script.sh``) 
    * specify a Python script ending in **.py**, and the appropriate command will be constructed automatically

Let's first write these mock applications in Python. Create a new folder and populate it with these Python files:

    >>> mkdir ~/balsam_tutorial
    >>> cd ~/balsam_tutorial

.. literalinclude:: balsam_tutorial/parent.py
    :caption: parent.py

.. literalinclude:: balsam_tutorial/parent-post.py
    :caption: parent-post.py

.. literalinclude:: balsam_tutorial/eigen.py
    :caption: eigen.py

.. literalinclude:: balsam_tutorial/reduce.py
    :caption: reduce.py


Writing dynamic workflows
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Notice that these are completely ordinary Python scripts, and the only
reference to Balsam is in ``parent-post.py``, where the ``launcher.dag`` API is
used to dynamically create the workflow and establish some data dependencies.
See :doc:`../userguide/dag` for more information on writing pre- and
post-processing scripts with Python.

The same effect can be achieved with job processing scripts in any other language.
In order to manipulate the BalsamJob database, your scripts can make use of system
calls to the Balsam utilities including ``balsam job``, ``balsam dep``, ``balsam killjob`` 
``balsam mkchild``. You can get help for the usage of these command-line tools by passing the
``--help`` flag.

Adding Applications to the Database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Now, let's create the ``ApplicationDefinitions`` for **parent.py**, **eigen.py**, and **reduce.py**
We can do this programatically with the Django ORM, but it usually makes more sense to define them
with the command-line tool:

    >>> $ balsam app --name parent --desc "generate matrices" --exec parent.py --postproc parent-post.py
    >>> $ balsam app --name eigen  --desc "compute eigenvalues" --exec eigen.py
    >>> $ balsam app --name reduce --desc "sort eigenvalues" --exec reduce.py

Use ``balsam app --help`` to get more information on how to construct ``ApplicationDefinitions`` from the command line.

Running the Workflow
---------------------

Now, all we have to do to kick off this dynamic workflow is add the parent job to the database:

    >>> $ balsam job --name parent1 --workflow balsam_tutorial --application parent --wall-min 1 --num-nodes 1 --processes-per-node 1

Assuming your Balsam environment allows you to run MPI locally with four ranks, try invoking
the Launcher:

    >>> $ balsam launcher --consume-all --max-ranks 4


Checking the workflow output
-------------------------------
You should very quickly see the ``balsam_tutorial`` subdirectory created in in
the ``data/balsamjobs`` subdirectory of the Balsam installation. This is the
main purpose of the ``--workflow`` field in BalsamJobs -- it allows you to
organize related jobs into named **workflows**.  In addition to capabilities for
querying and traversing DAGs by workflow, the job data is organized into
folders accordingly.

In the **parent** subdirectory (which includes part of a UUID suffix and looks
like ``balsamjobs/balsam_tutorial/parent1_db3abd55``) you should find the
generated matrix files and standard-output of the job and postprocessor script.


In the **eigen** subdirectories, you will just find one matrix file like
``output1.npy`` and one eigenvalues array file like ``eigvals0.npy``.  How did
Balsam know to transfer in this one matrix file? It was assigned to the job
dynamically in ``parent-post.py``.  The call to ``dag.spawn_child()`` created
one **eigen** job for each of the matrix files.  The ``input_files`` named
argument to ``spawn_child()`` specifies which files will be transferred from
parent to child job; it accepts wildcards and in fact defaults to ``*``, so
that *all* files are transferred from parent to child working directory.

.. note::
    When two BalsamJobs run in the same file system, symbolic links are created in the child subdirectory to input_files matched in the
    parent subdirectories. This choice was made to reduce the overhead and disk consumption of copying the same file, potentially many times.

Finally, the **reduce** subdirectory will contain ``results.dat`` which has the summary data we are looking for.
You can see a lot of files linked in to this directory, because it is a child of the **parent** and all **eigen** jobs, and
its ``input_files`` was left as the default value of ``*``.

Tracking Launcher execution
------------------------------
Now take a look in ``log/launcher.log``.  You will find many status messages allowing you to monitor the health of the Launcher 
(e.g. by searching for the word ``error`` or ``except``) and track the state of various jobs.  Since we ran the 
Launcher on our own laptop/workstation with 4 MPI ranks, we should be able to see how all of the **eigen** jobs ran concurrently,
being packaged into one ensemble. Search for the string "running serial jobs on" and you should find a couple lines like::

    21-Dec-2017 19:30:36|64440| INFO|balsam.launcher.runners:361] Running 5 serial jobs on 1 workers with 1 nodes-per-worker and 4 ranks per node
    21-Dec-2017 19:30:36|64440| INFO|balsam.launcher.runners:222] MPIEnsemble handling jobs:  [eigen0 | 21b8f515], [eigen1 | 25409657], 
                                [eigen2 | 9bfcf29c], [eigen3 | 5b5bd8a2], [eigen4 | fbd8c3c2]

                                
Balsam command line tools
------------------------------

You can always track the state of locally-running jobs with ``balsam ls``. It
comes with several options for filtering jobs and applications by name,
workflow, or ID.  There are concise list views and verbose job descriptions::
    
    $ balsam ls --hist                         # see the state history of each job
    $ balsam ls --name eigen --verbose         # only see eigen jobs (verbose)
    $ balsam ls --tree --wf balsam_tutorial    # see a Tree view of the DAG structure

Any argument can be abbreviated (e.g. --ver instead of --verbose) and searching by IDs matches the first few characters,
so you don't have to type out the entire UUID primary key. For instance, you can remove a job with ID ``fbd8c3c2-c9e7-4082-a0c5-4c9b9a716f59``
just by typing::

    $ balsam rm jobs --id fbd8
