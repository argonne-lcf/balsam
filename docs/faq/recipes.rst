Frequently Asked Questions
==========================

Why isn't the launcher running my jobs?
---------------------------------------------

Check the log for how many workers the launcher is assigning jobs to.  It may be
that a long-running job is hogging more nodes than you think, and there aren't enough
idle nodes to run any jobs.  Also, the launcher will only start running jobs that have an
estimated run-time below the allocation's remaining wall-time. 

Where does the output of my jobs go?
---------------------------------------

Look in the ``data/`` subdirectory of your :doc:`Balsam database directory
<../quick/db>`.  The jobs will be organized into folders according to the name
of their workflow, and each job working directory is in turn given a unique
name from its name and UUID.

All stdout/stderr from a job is directed into the file ``<jobname>.out``, along with job timing
information. Any files created by the job will be placed in its working directory, unless another
location is specified explicitly.

How can I move the output of my jobs to an external location?
--------------------------------------------------------------------

This is easy to do with the "stage out" feature of BalsamJobs.
You need to specify two fields, ``stage_out_url`` and ``stage_out_files``,
either from the ``balsam job`` command line interface or as arguments to
``dag.create_job()`` or ``dag.spawn_child()``.

stage_out_url
    Set this field to the location where you want files to go.  Balsam supports
    a number of protocols for remote and local transfers (scp, GridFTP, etc...). 
    If you just want the files to move to another directory in the same file system, use
    the ``local`` protocol like this::

        stage_out_url="local:/path/to/my/destination"

stage_out_files
    This is a whitespace-separated list of shell file-patterns, for example::

        stage_out_url='result.out *.log simulation*.dat'

    Any file matching any of the patterns in this field will get copied to the 
    ``stage_out_url``.

How can I control the way an application runs in my workflow?
------------------------------------------------------------------

There are several optional fields that can be set for each BalsamJob. These
fields can be set at run-time, during the dynamic creation of jobs, which
gives a lot of flexibility in the way an application is run. 

args
    Command-line arguments passed to the application

environ_vars
    Environment variables to be set for the duration of the application execution

input_files
    Which files are "staged-in" from the working directories of parent jobs. This
    follows the same shell file-pattern format as the ``stage_out_files`` field
    mentioned above. It is intended to facilitate data-flow from parent to child
    jobs in a DAG, without resorting to stage-out functionality.

preprocess and postprocess
    You can override the default pre- and post-processing scripts which run before and after
    the application is executed.  (The default processing scripts are defined alongside the application).

I want my program to wait on the completion of a job it created.
-----------------------------------------------------------------

If you need to wait for a job to finish, you can set up a polling function like the following::

    from balsam.launcher import dag
    import time

    def poll_until_state(job, state, timeout_sec=60.0, delay=5.0):
        start = time.time()
        while time.time() - start < timeout_sec:
            time.sleep(delay)
            job.refresh_from_db()
            if job.state == state:
                return True
        return False

Then you can check for any state with a specified maximum waiting time and delay. 
For finished jobs, you can do::

    newjob = dag.add_job( ... )
    success= poll_until_state(newjob, 'JOB_FINISHED')

There is a convenience function for reading files in a job’s working directory::

    if success:
        output = newjob.read_file_in_workdir(‘output.dat’) # contents of file in a string

Querying the Job database
---------------------------
You can perform complex queries on the BalsamJob database thanks to Django.  If
you ever need to filter the jobs according to some criteria, the entire
database is available via ``dag.BalsamJob``

See the `official documentation
<https://docs.djangoproject.com/en/2.0/topics/db/queries>`_ for lots of
examples, which directly apply wherever you can replace ``Entry`` with
``BalsamJob``.  For example, say you want to filter for all jobs containing
“simulation” in their name, but exclude jobs that are already finished::

    from balsam.launcher import dag
    BalsamJob = dag.BalsamJob
    pending_simulations = BalsamJob.objects.filter(name__contains=“simulation").exclude(state=“JOB_FINISHED”)

You could count this query::

    num_pending = pending_simulations.count()

Or iterate over the pending jobs and kill them::

    for sim in pending_simulations:
        dag.kill(sim)

Useful command lines
----------------------

Create a dependency between two jobs::

    balsam dep <parent> <child> # where <parent>, <child> are the first few characters of job ID

    balsam ls --tree # see a tree view showing the dependencies between jobs

Reset a failed job state after some changes were made::

    balsam modify jobs b0e --attr state --value CREATED # where b0e is the first few characters of the job id

See the state history of your jobs and any error messages that were recorded while the job ran::

    balsam ls --hist | less

Remove all jobs with substring "task"::
    
    balsam rm jobs --name task

Useful Python scripts
----------------------

You can use the ``balsam.launcher.dag`` API to automate a lot of tasks that
might be tedious from the command line.  For example, say you want to
**delete** all jobs that contain "master" in their name, but reset all jobs
that start with "task" to the "CREATED" state, so they may run again::

    import balsam.launcher.dag as dag

    dag.BalsamJob.objects.filter(name__contains="master").delete()

    for job in dag.BalsamJob.objects.filter(name__startswith="task"):
        job.update_state("CREATED")
