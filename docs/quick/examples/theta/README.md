Theta Example
-------------
This is a simple example of submitting a job on Theta which executes
the Balsam launcher that runs all of the ensemble jobs.
First, create a set of jobs to run using balsam qsub. Next, verify the
jobs are listed within the database using balsam ls. Finally, submit the
test job.sh script to execute the balsam lanucher.

.. code:: bash
    # build simple example
    cc -qopenmp -o hello.x ./hello.c

    # verify no jobs are found
    balsam ls jobs

    # queue work
    balsam qsub -n 1 -N 2 -t 0 -d 64 -j 2 $PWD/hello.x
    balsam qsub -n 2 -N 64 -t 0 -d 1 -j 1 $PWD/hello.x
    balsam qsub -n 2 -N 32 -t 0 -d 2 -j 1 $PWD/hello.x

    # verify jobs have been loaded
    balsam ls jobs

    # submit the job to theta
    qsub ./job.sh    


Balsam Status after Cobalt job completes
----------------------------------------

  job_id                               | name                       | workflow                   | application                | latest update
  --------------------------------------------------------------------------------------------------------------------------------------------
  c0315dc3-e0a2-42e5-80b4-9a743acc47b1 | default                    | qsub                       | /<...>/hpc-edge-service/docs/quick/examples/theta/hello.x | [01-12-2018 23:23:00 JOB_FINISHED]
  e8afc054-41c0-48e2-b64c-2b40e0358689 | default                    | qsub                       | /<...>/hpc-edge-service/docs/quick/examples/theta/hello.x | [01-12-2018 23:22:52 JOB_FINISHED]
  eed322bd-1a32-4777-b956-fd88435e89d0 | default                    | qsub                       | /<...>/hpc-edge-service/docs/quick/examples/theta/hello.x | [01-12-2018 23:22:44 JOB_FINISHED]

Job Output
----------
The output from the job.sh script is in 169162.output and 169162.error but
the output from the individual subjobs are located in the balsam data directory.

.. code:: bash
  ls $PWD/../../../../data/balsamjobs/qsub/

There is one directory for each subjob with the named `default_<job_id>` where
the job_id is shown in the balsam ls output.

.. code:: bash
  tail $PWD/../../../../data/balsamjobs/qsub/default_eed322bd/default.out

