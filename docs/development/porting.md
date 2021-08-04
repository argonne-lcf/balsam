# Porting Balsam to new HPC Sites

To port Balsam to a new system, a developer should only need to 
implement the following platform interfaces:

- `platform/app_run`: Add an `AppRun` subclass and include it in the `__init__.py`
- `platform/compute_node`: Same for `ComputeNode`
- `platform/scheduler`: Same for `SchedulerInterface`

A minimum of code is needed to support new batch schedulers (`SchedulerInterface`), compute node resources (`ComputeNode`), and MPI
application launchers (`AppRun`).  Developers should find and adapt existing implementations for systems already supported.

Next, create a new default configuration folder for the Site under `balsam/config/defaults`. 
This isn't strictly necessary (users can write their own config files),
but it makes it very convenient for others to quickly spin up a Site with the interfaces you wrote.

You will need the following inside the default Site configuration directory:

- `apps/__init__.py` (and other default apps therein)
- `settings.yml` (Referencing the platform interfaces added above)
- `job-template.sh`

The `job-template.sh` in particular must be written to be compatible with the form of batch job scripts
submitted to the HPC resource manager. Any BatchJob-wide scheduler flags, module loads (e.g. to ensure that 
internet access is available), should be present in the file.  The command containing the `{{ launcher_cmd }}` template should be copied as-is from existing job templates.  This is the line that will actually start the Balsam launcher.