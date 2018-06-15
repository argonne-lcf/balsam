Roadmap
========

This document outlines both short- and long-term goals for Balsam development.

Argo Service
---------------

* Establish Argo--Balsam communication protocol
    * Think carefully about scenarios where a job resides @Argo, @Balsam1, @Balsam2
    * Missing-jobs are easy: Argo can send jobs that are missing from a list of PKs
    * What if a job is out-of-date?  Simple timestamp or version number is not
      enough.  Do we enforce that only one instance can modify jobs? It would
      be very useful to let an ancestor job @Balsam1 modify the dependencies of
      some other job @Balsam2
* Balsam job sync service
* Balsam site scheduler: when a BalsamJob can run at more than one Balsam site
* Job source: automatic generation of BalsamJobs from some event (filesystem change, external DB write)
* Web interface

File Transfer Services
--------------------------

* Transparent data movement: data flow between Balsam sites
    * Stage-in logic should figure out where input files reside; choose appropriate protocol
    * Support Globus SDK, GridFTP, scp, cp, ln
* Simplify user_settings: uncluttered configuration/credentials

Balsam Jobs
---------------

* Database
    * SQLite + Global locks: likely to become a bottleneck.  Evaluate scalability with increasing writes.
    * DB that supports real concurrency
        * MySQL or Postgres?
        * DB installation, configuration, and running DB server should all be "hidden" from user in setuptools and Balsam service itself.  Keep the simplicity of user experience with SQLite.
    * Queueing save() events, manually implement a serialized writer process
* Implement script job type: for users who want to ``balsam qsub --mode script``
    * uses ScriptRunner that bypasses mpi
    * will need to parse user script, translate mpirun to system-specific commands
    * ensure script does not try to use more nodes than requested

Launcher Functionality
--------------------------

* Runner creation strategies: optimize the scheduling strategy given some runnable jobs and workers
    * MPIEnsemble
        * Right now it's fixed jobs per worker; try master-worker (job-pull) instead?
        * What if an MPIEnsembleRunner runs continuously, rather than being dispatched with a fixed set of jobs?
            * If there are also MPI jobs mixed in, we don't want it to hog resources
        * How many serial job ranks per node?
    * How long to wait between Runner creations
* Transitions
    * Move to Metascheduler service? Do the transitions really need to be happening on the Launcher side?
        * Especially makes sense if there is heavy data movement; why wait to stage-in until compute allocation!?
    * Improving concurrency with coroutines?
        * We could keep 1-10 transition processes, but increase the concurrency in each by using asynchronous I/O for DB read/writes, and especially time-consuming stage-in/stage-out transfers
        * Rewriting transition functions as coroutines may be a very natural choice
    * PriorityQueue implementation: the Manager process incurs some overhead; how much faster than FIFO?
* Currently Launcher runs in place of user-submitted batch scripts; this happens on a single "head" node of an allocation, from which MPI applications are initiated. This design choice is based on current constraints at ALCF.  How can/should this improve with future abilities like:
    * Containerized applications with Singularity
    * direct ssh access to compute nodes
    * MPI Spawn processes
* Support BG/Q workers: worker setup() routine should boot subblocks of requested size
* Wrap post-process steps (which run arbitrary user code) in a transaction, so that DB can be rolled back if
  the Launcher crashes in the middle of some DAG manipulations

Metascheduler Service
-------------------------

* Argo--Balsam job sync service
    * periodically send request for new jobs, updates
* Abstractions for queue queries, application performance models, scheduling strategy
* Bayesian updates of performance model from application timing data
* Aggressive search for holes in local batch queue
* Scheduling algorithms to minimize time-to-solution
    * Constrained by maximum node-hour usage, minimum parallel efficiency

User Experience
-----------------

* Move hard-coded logic out of user_settings.py: simplify user configuration
* Update documentation
* Document prototype/patterns of Brain Imaging workflow
* Document prototype/patterns of Solar Windows workflow
* Document prototype/patterns of Hyperparameter Opt workflow
* Python CWL Parser: automatic workflow generation from CWL script

