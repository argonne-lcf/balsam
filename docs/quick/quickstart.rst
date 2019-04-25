Get Balsam
==========

On Theta
---------
The fastest way to get Balsam on Theta is to load the module. 

.. highlight:: console

::
    
    $ module load balsam
    $ which balsam
    /soft/datascience/Balsam/0.3.5.1/env/bin/balsam
    $ which python
    /soft/datascience/Balsam/0.3.5.1/env/bin/python

As you can see, the module loads a Python 3.6 environment with pre-configured Balsam installation. 
Don't worry if your applications use different versions of Python in other environments. You can simply
use the Balsam Python environment to set up and manage your workflows, while a different version of Python or 
any application, for that matter, runs in the backend.

.. highlight:: python

::
    
    >>> from balsam.launcher import dag
    >>> job = dag.current_job
    >>> job.state
    "RUN_ERROR"

That's it.

.. highlight:: console

::

    $ . balsamactivate ~/testdb/
    Server at Mishas-MacBook-Pro:63462 isn't responsive; will try to kill and restart
    Stopping local Balsam DB server
    pg_ctl: PID file "/Users/misha/testdb/balsamdb/postmaster.pid" does not exist
    Is server running?
    Launching Balsam DB server
    waiting for server to start.... done
    server started
    [BalsamDB: testdb] $
    [BalsamDB: testdb] $ balsam ls
                                  job_id |    name | workflow | application |  state
    --------------------------------------------------------------------------------
    f59d0d04-97e5-4d6b-aa4e-d2d51c2b4f6f | fail199 | test2    | failer      | FAILED
    78c5e168-9b4d-4598-81e1-824c5ce5a47c | fail118 | test2    | failer      | FAILED
    1a95da86-6bc8-4a2c-823c-c41970471f23 | fail24  | test2    | failer      | FAILED
    f6376e28-9949-4ec7-aa48-15c7023aaf1f | fail63  | test2    | failer      | FAILED
    2c2ab35c-afbc-4a8f-8e98-a0f235859ed7 | fail51  | test2    | failer      | FAILED
    ef25b672-6383-4bfb-8e37-3a3a6783d4d7 | fail108 | test2    | failer      | FAILED
    73ba9764-49ad-48a5-97b5-61c278b0f85b | fail13  | test2    | failer      | FAILED
    600d4d9e-e6bb-4a28-ab61-dfa1b4d8d84f | fail12  | test2    | failer      | FAILED
    225f04cd-0cbb-4a43-aeec-dfb87b0fe5ab | fail134 | test2    | failer      | FAILED
    5bad88d2-9a97-4c82-8a4d-24bdaa9e2da4 | fail1   | test2    | failer      | FAILED
    9894e79b-e70a-4efe-a8df-18eb11076539 | fail167 | test2    | failer      | FAILED
    93316efa-4bf5-463f-8296-de0e8b2d2355 | fail92  | test2    | failer      | FAILED
    71b4de2b-58ad-47f0-8d8a-e17d778675f3 | fail125 | test2    | failer      | FAILED

thats all folks.


Building documentation
------------------------
You can use Sphinx to build this documentation from source and view it locally in your web browser:

.. code:: bash

        $ pip install --user sphinx sphinx-rtd-theme
        $ cd docs
        $ make html
        $ firefox _build/html/index.html
        

Prerequisites
-------------
Balsam requires **Python 3.6** and **mpi4py** to be set up and running prior to installation.
The best approach is to set up an Anaconda environment just for Balsam. This environment can
easily be cloned or extended to suit the needs of your own workflows that use Balsam.

.. note:: 
    A working **mpi4py** installation is somewhat system-dependent and therefore this
    dependency is not packaged with Balsam. Below are guidelines to get it set up
    on a few systems where Balsam has been tested.

Mac OS X 
^^^^^^^^^^
First, `get Anaconda <https://www.anaconda.com/download>`_. Then, use Anaconda
to get the Intel Python 3.6 distribution, and use pip to install a different version 
of mpi4py:

.. code:: bash

    $ conda config --add channels intel
    $ conda create --name balsam intelpython3_full python=3
    $ source activate balsam
    $ pip install mpi4py # otherwise mpi4py doesn't work

Cooley (@ALCF)
^^^^^^^^^^^^^^^^^^^^^^^
.. code:: bash

    $ soft add +anaconda
    $ conda config --add channels intel
    $ conda create --name balsam_cooley intelpython3_full python=3
    $ source activate balsam_cooley # mpi4py just works

Theta (@ALCF)
^^^^^^^^^^^^^^^^^^^^^^^
.. code:: bash

    $ export PATH=$PATH:$HOME/bin:/opt/intel/python/2017.0.035/intelpython35/bin # add to .bash_profile
    $ conda config --add channels intel
    $ conda create --name balsam intelpython3_full python=3
    $ source activate balsam
    $ cp  /opt/cray/pe/mpt/7.6.0/gni/mpich-intel-abi/16.0/lib/libmpi*  ~/.conda/envs/balsam/lib/ # need to link to intel ABI
    $ export LD_LIBRARY_PATH=~/.conda/envs/balsam/lib:$LD_LIBRARY_PATH # add to .bash_profile

.. warning:: 
    If running on Balsam on two systems with a shared file system, keep in mind
    that a **separate** conda environment should be created for each (e.g.
    balsam_theta and balsam_cooley).

Environment
-----------
Before installing Balsam, and whenever you subsequently use it, remember the appropriate
environment must be loaded! Thus, for every new login session or in each job submission script, be sure
to do the following:

Mac OS X
^^^^^^^^^

.. code:: bash

    source activate balsam

Cooley
^^^^^^^^^

.. code:: bash

    soft add +anaconda
    source activate balsam_cooley

Theta
^^^^^^^^^

.. code:: bash

    source ~/.bash_profile # this is not auto-sourced on MOM nodes
    source activate balsam


Get Balsam
-----------
Check out the development branch of Balsam:

.. code:: bash

    git clone git@xgitlab.cels.anl.gov:turam/hpc-edge-service.git
    cd hpc-edge-service
    git checkout develop

Pip/setuptools will take care of the remaining dependencies (``django``, etc...) and run the 
necessary code to set up the default Balsam database.

.. code:: bash

    pip install -e . # your balsam environment is already loaded

Quick Tests
-------------
The ``balsam-test`` command-line utility will have been added to your path.  To
check the installation, try running one of the quick tests.  The ``--temp`` parameter
creates a temporary test database for the duration of the unit tests::

    $ balsam-test --temp tests.test_dag

