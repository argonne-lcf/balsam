# Porting Balsam to new HPC Sites

Porting Balsam to a new system requires minimal (or no) code.
We simply need to provide an off-the-shelf **default configuration** that
users of the system can bootstrap new Sites from. 

## Select the Platform Interfaces

To port Balsam to a new system, one only needs to select *three* compatible interfaces:

1.  `AppRun`: The MPI application launcher class
2.  `ComputeNode`: The node resource definition class
3.  `SchedulerInterface`: The HPC resource manager (batch scheduler) class

Several interfaces are implemented in the respective platform directories:
`platform/app_run`, `platform/compute_node`, and `platform/scheduler`.  If the
interface to your system is missing, simply add a new subclass that copies the
structure of an existing, related implementation.  In most cases, the necessary
changes are minimal.  New interfaces should be included in the appropriate
`__init__.py` for uniform accessibility.

## Create a Default Configuration

Create a new configuration folder for your platform under `balsam/config/defaults/`.
Inside, you will need to add the following:

- `apps/__init__.py` (and other default apps therein)
- `settings.yml` (Referencing the platform interfaces added above)
- `job-template.sh`

Again, the easiest way is to copy an example from one of the existing folders in `balsam/config/defaults/`.
The `job-template.sh` is used to generate the shell scripts that are ultimately submitted to the HPC scheduler.
This is where any necessary scheduler flags or `module load` statements can be added.