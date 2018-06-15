# HPC Edge Service and Workflow Management System
**Authors:** J. Taylor Childers (Argonne National Laboratory), Tom Uram (Argonne National Laboratory), Doug Benjamin (Duke University), Misha Salim (Argonne National Laboratory)

# Prerequisites
The user is responsible for providing an environment with Python 3.6 and mpi4py, because the installation is
system-dependent. 

## Prerequisites on Cooley
An easy approach is to use Anaconda:
```
soft add +anaconda
conda config --add channels intel
conda create --name balsam intelpython3_full python=3
source activate balsam
```
On Cooley, mpi4py just works with this environment.
The following instructions assume the appopriate environment for Balsam is set-up and loaded!


# Check out the latest release of Balsam
```
git clone git@xgitlab.cels.anl.gov:turam/hpc-edge-service.git
cd hpc-edge-service
git checkout release0.1
```

# Install Balsam
```
pip install -e .
```

# Try it out!
The launcher pulls jobs from the database and invokes MPI to run the jobs.
To try it out interactively, grab a couple nodes on Cooley:
```
qsub -A datascience -n 2 -q debug -t 30 -I
soft add +anaconda
source activate balsam
```

The **balsam** command-line tool will have been added to your path.
There are a number of commands to try:
```
balsam --help
balsam ls --help
balsam ls # no jobs in DB yet
```

Now let's create a couple dummy jobs and see them listed in
the database:
```
balsam qsub "echo hello world" --name hello -t 0
balsam make_dummies 2

balsam ls --hist 
```

Finally, run the launcher. Useful log messages will be sent to the log/ directory in real time.
You can change the verbosity, and many other Balsam runtime parameters, in balsam/user_settings.py

```
balsam launcher --consume --time 0.5 # run for 30 seconds
balsam ls --hist # jobs are now done
balsam rm jobs --all
```

# Run a comprehensive test suite
The **balsam-test** command line tool invokes tests in the tests/ directory
You can run specific tests by passing the test module names, or run all of
them just by calling **balsam-test** with no arguments.
```
balsam-test tests.test_dag # this should be quick
balsam-test # the test_functional module might take over 10 minutes!
```
