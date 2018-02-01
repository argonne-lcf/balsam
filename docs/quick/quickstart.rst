Install Balsam
==========================

.. note::
    If you are reading this documentation from GitHub/GitLab, some of the example code
    is not displayed!  Take a second and build this documentation on your
    own machine (until it's hosted somewhere accessible from the internet)::

        $ pip install --user sphinx
        $ cd docs
        $ make html
        $ firefox _build/html/index.html # or navigate to file from browser
        

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

