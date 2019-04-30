Hello World and Testing
=========================

Hello World (on Cooley)
------------------------
The launcher pulls jobs from the database and invokes MPI to run the jobs.
To try it out interactively, grab a couple nodes on Cooley and remember to
load the appropriate environment::

    qsub -A datascience -n 2 -q debug -t 30 -I
    soft add +anaconda
    source activate balsam

The **balsam** command-line tool will have been added to your path.
There are a number of sub-commands to try; to explore the options, use 
the ``--help`` flag::

    balsam --help
    balsam ls --help

Let's setup a balsam DB in our home directory for testing and start up a DB server to
manage that database::
    
    balsam init ~/testdb
    export BALSAM_DB_PATH=~/testdb
    balsam dbserver --path ~/testdb
    

With the ``BALSAM_DB_PATH`` environment variable set, all ``balsam`` programs will refer to this
database.  Now let's create a couple dummy jobs and see them listed in
the database::

    balsam ls # no jobs in ~/testdb yet
    balsam qsub "echo hello world" --name hello -t 0
    balsam make_dummies 2
    balsam ls --hist  # history view of jobs in ~/testdb

Useful log messages will be sent to ``~/testdb/log/`` in real time. You can
change the verbosity, and many other Balsam runtime parameters, in
balsam/user_settings.py. Finally, let's run the launcher::

    balsam launcher --consume --time 0.5 # run for 30 seconds
    balsam ls --hist # jobs are now done
    balsam ls --verbose 
    balsam rm jobs --all


Comprehensive Test Suite
------------------------
The **balsam-test** command line tool invokes tests in the ``tests/`` directory
You can run specific tests by passing the test module names, or run all of them
just by calling **balsam-test** with no arguments. You can provide the ``--temp`` parameter
to run certain tests in a temporary test directory::

    $ balsam-test tests.test_dag --temp # this should be quick

You should see this at the end of the test output::

    ----------------------------------------------------------------------
    Ran 3 tests in 1.575s

    OK

To run the comprehensive set of unit and functional tests, you must create a
persistent test DB and run a DB server in front of it.  To help prevent users
from running tests in their production database, Balsam requires that the DB
directory contains the substring "test". (The ``~/testdb`` DB created above
would suffice here).  Be sure that a DB server is running in front of the test
database::

    export BALSAM_DB_PATH=~/testdb
    balsam dbserver --path ~/testdb
    balsam-test # the test_functional module might take over 10 minutes!
