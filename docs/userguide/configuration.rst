.. role:: bash(code)
   :language: bash

Configuration
==============

On using Balsam, a `.balsam` configuration directory is created in
your home folder. This contains an adjustable `settings.json` to control 
how several Balsam components behave.  The default should look like
the following:

.. code-block:: javascript
    :caption: **~/.balsam/settings.json**
    
    {
        "SCHEDULER_CLASS": "CobaltScheduler",
        "SCHEDULER_SUBMIT_EXE": "/usr/bin/qsub",
        "SCHEDULER_STATUS_EXE": "/usr/bin/qstat",
        "DEFAULT_PROJECT": "datascience",
        "SERVICE_PERIOD": 1,

        "NUM_TRANSITION_THREADS": 5,
        "MAX_CONCURRENT_MPIRUNS": 1000,

        "LOG_HANDLER_LEVEL": "INFO",
        "LOG_BACKUP_COUNT": 5,
        "LOG_FILE_SIZE_LIMIT": 104857600,

        "QUEUE_POLICY": "theta_policy.ini",
        "JOB_TEMPLATE": "job-templates/theta.cobaltscheduler.tmpl"
    }


- **DEFAULT_PROJECT**: If you want to use the Balsam service for automated job
  submission, you will need to set this to control which allocation your jobs
  are submitted to.
- **NUM_TRANSITION_THREADS**: Control how many processes run alongside each
  launcher to run data staging and pre/post-processing tasks. If these are not
  relevant to your workflow, you can reduce this value to 1.
- **JOB_TEMPLATE**: Relative path to the job script template that is used to
  submit launcher jobs. You may modify this template to insert your own pre-run
  logic or create a new template and point to it with this field.

.. _JobTemplate:

Customizing the Job Template
------------------------------
:ref:`ApplicationDefinition` shows how `ApplicationDefinitions` can set the
environment and pre/post-processing scripts for each application in your
workflow. If you want to run some commands at the start of **every job** 
or set **global** environment variables, you can insert this logic in the job template.

For instance, suppose that `~/libs/myLib` needs to be copied to a local SSD
on each compute node at the start of every job in your workflow. On the ALCF
Theta platform, this is easily accomplished by inserting one line:
:bash:`aprun -n $COBALT_JOBSIZE -N 1 cp -r ~/libs/myLib /local/scratch` before the 
`balsam launcher` command is invoked to start executing your workflow.  A complete
example is shown below.

.. code-block:: bash
    :caption: **~/.balsam/job-templates/theta.cobaltscheduler.tmpl**

    #!/bin/bash -x
    #COBALT -A {{ project }}
    #COBALT -n {{ nodes }}
    #COBALT -q {{ queue }}
    #COBALT -t {{ time_minutes }}
    #COBALT --attrs ssds=required:ssd_size=128

    export PATH={{ balsam_bin }}:{{ pg_bin }}:$PATH

    module unload trackdeps
    module unload darshan
    module unload xalt
    export PMI_NO_FORK=1 # otherwise, mpi4py-enabled Python apps with custom signal handlers do not respond to sigterm

    # *** CUSTOM PRE-RUN LOGIC HERE: ***
    aprun -n $COBALT_JOBSIZE -N 1 cp -r ~/libs/myLib /local/scratch  

    # ***

    source balsamactivate {{ balsam_db_path }}
    sleep 2
    balsam launcher --{{ wf_filter }} --job-mode={{ job_mode }} --time-limit-minutes={{ time_minutes-2 }}

    source balsamdeactivate

.. warning::
    It is best to avoid exporting global `LD_LIBRARY_PATH` or `PYTHONPATH`
    variables in your Job Template, because these are inherited by all tasks
    in the workflow and often clobber how shared libraries or Python packages
    are loaded, respectively. 
    `If your codes are properly linked <http://xahlee.info/UnixResource_dir/_/ldpath.html>`_ 
    and Python packages are properly installed into isolated environments, it is rarely
    necessary to set these variables. If you must, it's much better to set
    them in the Application `envscript` to prevent polluting the global
    environment (see :ref:`ApplicationDefinition`).