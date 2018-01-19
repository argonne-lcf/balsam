Install Balsam
==========================

Prerequisites
-------------
Balsam requires **Python 3.6** and **mpi4py** to be set up and running prior to installation.
The best approach is to set up an Anaconda environment just for Balsam.

.. note:: 
    A working **mpi4py** installation is somewhat system-dependent and therefore this
    dependency is not packaged with Balsam. Below are just guidelines to get it set up
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
    $ conda create --name balsam intelpython3_full python=3
    $ source activate balsam # mpi4py just works

Theta (@ALCF)
^^^^^^^^^^^^^^^^^^^^^^^
.. code:: bash

    $ export PATH=$PATH:$HOME/bin:/opt/intel/python/2017.0.035/intelpython35/bin # add to .bash_profile
    $ conda config --add channels intel
    $ conda create --name balsam intelpython3_full python=3
    $ source activate balsam
    $ cp  /opt/cray/pe/mpt/7.6.0/gni/mpich-intel-abi/16.0/lib/libmpi*  ~/.conda/envs/balsam/lib/ # need to link to intel ABI
    $ export LD_LIBRARY_PATH=~/.conda/envs/balsam/lib:$LD_LIBRARY_PATH # add to .bash_profile

.. note:: 
    If running on Balsam on two systems with a shared file system, keep in mind
    that a separate conda environment should be created for each (e.g.
    balsam_theta and balsam_cooley)

Get Balsam
-----------
.. code:: bash

    git clone git@xgitlab.cels.anl.gov:turam/hpc-edge-service.git
    cd hpc-edge-service
    git checkout develop

Installation
-------------
Pip/setuptools will take care of the remaining dependencies (``django``, etc...) and run the 
necessary code to set up a default Balsam database.

.. code:: bash

    pip install -e .

Quick Tests
-------------
The ``balsam-test`` command-line utility will have been added to your path.  To check the installation, try
running one of the quick tests:

    >>> balsam-test tests.test_dag

Hello World (on Cooley)
------------------------
The launcher pulls jobs from the database and invokes MPI to run the jobs.
To try it out interactively, grab a couple nodes on Cooley::

    qsub -A datascience -n 2 -q debug -t 30 -I
    soft add +anaconda
    source activate balsam

The **balsam** command-line tool will have been added to your path.
There are a number of sub-commands to try; to explore the options, use 
the ``--help`` flag::

    balsam --help
    balsam ls --help
    balsam ls # no jobs in DB yet

Now let's create a couple dummy jobs and see them listed in
the database::

    balsam qsub "echo hello world" --name hello -t 0
    balsam make_dummies 2
    balsam ls --hist 

Finally, run the launcher. Useful log messages will be sent to the log/ directory in real time.
You can change the verbosity, and many other Balsam runtime parameters, in balsam/user_settings.py::

    balsam launcher --consume --time 0.5 # run for 30 seconds
    balsam ls --hist # jobs are now done
    balsam rm jobs --all

Hello World (on Theta)
------------------------
The procedure is largely the same as for Cooley, except that instead of using "soft", anaconda
is added to the PATH explicitly::
    $ qsub -A datascience -n 2 -q debug-cache-quad -t 30 -I
    $ source ~/.bash_profile # this should contain the PATH export mentioned previously
    $ source activate balsam
    $ export LD_LIBRARY_PATH=~/.conda/envs/balsam/lib:$LD_LIBRARY_PATH # if not already in .bash_profile
    $ balsam ls # ready to go

Comprehensive Test Suite
------------------------
The **balsam-test** command line tool invokes tests in the tests/ directory
You can run specific tests by passing the test module names, or run all of
them just by calling **balsam-test** with no arguments::

    balsam-test tests.test_dag # this should be quick
    balsam-test # the test_functional module might take over 10 minutes!

