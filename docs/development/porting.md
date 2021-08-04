# Porting Balsam to new HPC Sites

To port Balsam to a new system, a developer should only need to 
implement the following platform interfaces:

- `platform/app_run`: Add AppRun subclass and list it in the __init__.py
- `platform/compute_node`: Same for ComputeNode
- `platform/scheduler`: Same for SchedulerInterface

Then create a new default configuration folder for the Site under `balsam/config/defaults`.  This isn't strictly necessary (users can write their own config files) but it makes it very convenient for others to quickly spin up a Site with the interfaces you wrote.  

You will need the following inside the default Site configuration directory:

- `apps/__init__.py` (and other default apps therein)
- `settings.yml` (Referencing the platform interfaces added above)
- `job-template.sh`
