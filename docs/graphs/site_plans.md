Job
----
Let workdir uniqueness be the user's problem.
If they put 2 jobs with same workdir, assume its intentional.  We can ensure that "stdout" of each job goes into a timestamped file, so multiple runs do not collide.  Name of stdout file can be stored in DB for easy retrieval.

Workdir existence is ensured/double-checked before Stage-in, Preproc, Run
Launcher exit criterion: simplify to nothing run for 60 sec

Acquisition Strategy
--------------------
Acqusition/Stage-in module:
    Acquring READY jobs (include_unbound=True) and updating to STAGED_IN
    - count: num_running, num_ready, num_staged_in
    - backlog = num_staged_in + num_ready
    - settings: 
        - stage-in-batch-size
        - max-active-transfers
        - max-backlog: const or callable
            - heuristic: max(20, num_running)
        if num_active_transfers < max_active transfers:
            if backlog < max_backlog:
                num_to_acquire = min(max_backlog-backlog, batch_size)
                acquire(state=READY, include_unbound=True, max_acquire=num_to_acquire)
                at this point, jobs are bound to the site
            
        transfer_tasks = collect_transfer_tasks(acquired_jobs)
        TransferThreadPool.submit(transfer_tasks)
            - a thread tracks the set of transfer tasks submitted to it
            - submits and monitors Globus transfer job
            - does batch-update of job transfer task states

Preprocess module: 
    Acquiring STAGED_IN jobs and updating to PREPROCESSED (bound only)
    acquire(states=[staged_in, done, error, timeout], bound only, limit=1000)
    once a job is bound and staged-in; no need to throttle preprocess or postprocess

Postprocess module:
    Acquiring: done, error, timeout jobs
    Updating to: postprocessed, restart_ready, failed

Stage-out module:
    Acquiring POSTPROCESSED jobs, updating to JOB_FINISHED

Acqusition module has its own JobSource (providing backpressure)
Preprocess, Postprocess, Stageout can share one JobSource
All four modules can share one StatusUpdater
Total: 2 job sources, 1 statusupdater

Scheduler module:
    Periodically updates Site nodelist & full job queues
    Syncs BatchJobs (see below) and interfaces to system Scheduler

Auto-submitter:
    Automates BatchJob submission (fixed size or backfill sniper) 
    Backfill sniper: Deletes BatchJobs that did not go through within N min
    counting running, preprocessed, staged_in
    backlog = staged_in + preprocessed
    submit based on: 
        node requirements of runnable jobs (how many preprocessed or restartable)
        number of currently running nodes
        number of queued nodes
    
BatchJobs
----------
Balsam service sync protocol:
    1) GET list of currently active batchjobs
    2) Get "qstat"
    3) for each active batchjob, compare with the qstat record and take action
       1) "pending_submission" - qsub, update scheduler_id, state="queued"
            - on submission error, status_message=error and state="submit-failed" 
       2) "pending_deletion" 
          - if in qstat list: qdel (leave it as "pending-deletion" until its actually gone)
          - if not in qstat list (i.e. job cleanly exited or it never ran), set state=finished; parse log for start&end time
       3) on any status change, update the state
       4) if a job is finished for any reason, parse the logs to update start-time/end-time, etc...
       5) if any parameters like walltime were changed; run qalter if possible, if it's too late; revert the change
    4) Apply bulk-update via PATCH on list view
        - for any "running" job, add revert=True to the patch dict. this enforces that running job parameters are consistent

App
===
Each App backend is a balsam.models.App subclass
These classes strictly reside in apps/ subdirectory of a Site

When a user registers an app:
    balsam agent must not be running
    the app is validated (no import/syntax error) 
    the app and its parent folder are set to read-only permission
    (if applicable, chattr +i)
Balsam agent *refuses* to run if an app permission is anything other than 400 or parent directory permission is anything other than 500

Security: authentication must be via ALCF 2-factor OAuth.  Once authenticated, a 48-hour access token is written into hidden home directory with user read-only (400) permissions. If a user wants to run Balsam agent continously on a site, they will need to physically log in and re-authenticate to refresh the token once a day.

Security: All aspects of the App's execution are contained in the Site-local file and NEVER stored or accessed by the public API:
    - Environment variables
    - Preamble (module loads, etc..)
    - Preprocess fxn
    - Postprocess fxn
    - Command template & parameters
    - Error & timeout handling options

Security: none of these can be accessed via API; the User has to be physically logged-in to define the Applications that can run

Security: the command is generated from a Jinja template that only takes
pre-specified parameters.  The parameters are properly escaped to prevent shell
injection. "shutil.which" must verify that the first argument is a valid executable on the filesystem.  (Alternatively, the command is broken into arg-list which runs as subprocess with shell=False; preventing command injection)

Security: all job processing must take place in job workdir.  The job workdirs are strictly under the data/ subdirectory. Any stage-in destination is expressed as an absolute-path to the workdir. This prevents staging-in malicious code into apps/

Security: If balsam is running, then apps/ and all files therein are permission Read-only. This means that the applications a Balsam agent can run may never be modified during execution. This prevents a "stage-out" from over-writing an Application definition. 