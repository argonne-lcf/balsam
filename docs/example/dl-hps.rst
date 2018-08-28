Hyperparameter Optimization
===============================

.. note::
    The code examples below are heavily pruned to illustrate only essentials of
    the Balsam interface. Refer to the 
    `dl-hps repo <https://xgitlab.cels.anl.gov/pbalapra/dl-hps/tree/balsam-port>`_ 
    for runnable examples and code. 

In this workflow, a light-weight driver runs either on a login node, or as a
long-running, single-core job on one of the compute nodes.  The driver executes
a hyperparameter search algorithm, which requires that the model loss is
evaluated for several points in hyperparameter space. It evaluates these points
by dynamically adding jobs to the database:: 

    import balsam.launcher.dag as dag
    from balsam.service.models import BalsamJob, END_STATES

    def create_job(x, eval_counter, cfg):
        '''Add a new evaluatePoint job to the Balsam DB'''
        task = {}
        task['x'] = x
        task['params'] = cfg.params

        jname = f"task{eval_counter}"
        fname = f"{jname}.dat"
        with open(fname, 'w') as fp: fp.write(json.dumps(task))

        child = dag.spawn_child(name=jname, workflow="dl-hps",
                    application="eval_point", wall_time_minutes=2,
                    num_nodes=1, ranks_per_node=1,
                    input_files=f"{jname}.dat", 
                    args=f"{jname}.dat",
                    wait_for_parents=False
                   )
        return child.job_id

In this function, the point is represented by the Python dictionary ``x``. This
and other configuration data is dumped into a JSON dictionary on disk. The job
to evaluate this point is created with ``dag.spawn_child`` and the
``wait_for_parents=False`` argument, which indicates that the child job may
overlap with the running driver job. The job carries out application ``"eval_point"``,
which is pre-registered as the worker application.

The ``input_files`` argument is set to the name of the JSON file, which causes
it to be transferred into the working directory of the child automatically.
Also, ``args`` is set to this filename in order to pass it as an
argument to the ``eval_point`` application.

The driver itself makes calls to this ``create_job`` function in order to
dispatch new jobs.  It also queries the Balsam job database for newly finished
jobs, in order to assimilate the results and inform the optimizer::

    while not opt_is_finished():
        
        # Spawn new jobs
        XX = opt.ask(n_points=num_tocreate) if num_tocreate else []
        for x in XX:
            eval_counter += 1
            key = str(x)
            jobid = create_job(x, eval_counter, cfg)
            my_jobs.append(jobid)

        # Read in new results
        new_jobs = BalsamJob.objects.filter(job_id__in=my_jobs)
        new_jobs = new_jobs.filter(state="JOB_FINISHED")
        new_jobs = new_jobs.exclude(job_id__in=finished_jobs)
        for job in new_jobs:
            result = json.loads(job.read_file_in_workdir('result.dat'))
            resultsList.append(result)
            finished_jobs.append(job.job_id)

The strength of Balsam in this workflow is decoupling the optimizer almost
entirely from the evaluation of points.  The ``eval_point`` jobs take some
input JSON specification; besides this, they are free to run arbitrary code as
single-core or multi-node MPI jobs.  The Balsam launcher and job database allow
an ensemble of serial and MPI jobs to run concurrently, and they are robust to
allocation time-outs or unexpected failures. The driver and/or jobs can be
killed at any time and restarted, provided the driver itself is checkpointing
data as necessary.
