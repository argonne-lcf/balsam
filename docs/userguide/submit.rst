Submitting Jobs
=================
The `balsam launcher` command starts running the launcher component inside of a
scheduled job. The launcher automatically detects the allocated resources and 
begins executing your workflows.

However, you typically **do not** run `balsam launcher` yourself, unless you are
working interactively to test and debug workflows. From a login node,
there are two ways to submit jobs that run `balsam launcher` for you:

1. :bash:`balsam submit-launch` -- manually or automatically with the `balsam service`, 
   a Bash script is generated from the Job Template, saved in the `qsubmit`
   subdirectory of your database, and submitted to the scheduler on your
   behalf.

2. :bash:`balsam service` -- start up the **service** daemon, which persists
   on the login node and submits launcher jobs on your behalf over time.

In either case, the following takes place when you submit with Balsam:
- A bash script is generated from the Job Template (see
:ref:`JobTemplate` for an easy way to "hook-in" custom data movement or other
pre-run commands) 
- The script is named :bash:`qlaunch<id>.sh` and saved in the :bash:`qsubmit/` subdirectory of your database 
- The script is submitted to the batch scheduler using the platform's scheduler interface 
  (e.g. via :bash:`qsub`)

Manually Submit with **submit-launch**
---------------------------------------