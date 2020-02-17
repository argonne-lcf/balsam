# NWChem: Parameter Sweep and Pre/Post-processing

Even if you are not interested in Chemistry, this tutorial illustrates
several important Balsam concepts:

  -   Setting up an Application that requires several modules to run
  -   Pre-processing to generate input files
  -   Post-processing to parse and store calculation results
  -   Storing JSON data with PostgreSQL
  -   Creating a large parameter sweep-type ensemble
  -   Adding a reduce-type (summary) job using `dag.add_dependency()`

Water HF/6-31G Potential Energy Scan
------------------------------------

In this exercise, we\'ll use NWChem on Theta to calculate the electronic
ground state energy of the water molecule. We would like to repeat this
calculation many times as a function of the symmetric O\--H bond
stretching distance, in order to generate a 1-dimensional potential
energy surface (PES).

Let\'s see how an existing build of NWChem on Theta (courtesy of Alvaro
Vazquez-Mayagoitia) can be used in a Balsam workflow to generate the
water PES.

Setting Up
----------

Let\'s create a clean workspace and Balsam DB for this exercise as
follows.

``` {.bash}
$ rm -r ~/.balsam  # reset default settings (for now)
$ mkdir ~/tut-nwchem
$ cd ~/tut-nwchem
$ module load balsam
$ balsam init db
$ . balsamactivate db
```

The NWChem Application
----------------------

Suppose we were looking for a public build of NWChem on Theta. We search
in `/soft/applications`{.interpreted-text role="bash"} and find the
binary `/soft/applications/nwchem/6.8/bin/nwchem`{.interpreted-text
role="bash"} alongside a `submit.sh`{.interpreted-text role="bash"}
script to launch a run. In order for NWChem to run properly, this script
loads some modules and sets many MPICH environment variables. The
easiest way to set up this environment for NWChem in Balsam is to create
a file with all the *module load* and *export* statements, and set this
file as the **envscript** (Environment Script) of the NWChem
Application. We first copy over the file:
`cp /soft/applications/nwchem/6.8/bin/submit.sh envs.sh`{.interpreted-text
role="bash"} and then delete the launch commands, so it\'s only setting
up the environment and looks as follows:

::: {.highlight}
bash
:::

::: {.literalinclude caption="envs.sh"}
tut-nwchem/envs.sh
:::

Instead of defining applications from the command line, we can write a
small Python script to generate the two **ApplicationDefinitions**.

::: {.highlight}
python
:::

::: {.literalinclude caption="apps.py"}
tut-nwchem/apps.py
:::

You\'ll notice this script provides pre- and post-processing scripts for
**nwchem-water**, as well as a second **plot-pes** app. We will get to
these shortly. First, let\'s write this **plot-pes** script.

The \"Plotting\" Application
----------------------------

After our PES scan is completed, we would like to generate a summary
plot of the data. This can be accomplished by adding a **plot-pes** task
as the child of all the **nwchem-water** tasks. Creating these
dependencies will form a DAG, as shown in the figure below. The plot
step only runs after all the parent NWChem calculations have finished
succesfully.

![The DAG consists of several independent NWChem single-point energy
tasks, followed by a \"reduce\" Plotting task that depends on successful
completion of all the energy
calculations.](tut-nwchem/dag.png){.align-center}

Now create the plot script as shown below. The highlighted lines show
how the task will find its parent tasks through the Python API. The
results are organized by pulling O\--H bond length (*r*) and the
electronic energy (*energy*) from each nwchem-water task\'s **data**
dictionary.

::: {.highlight}
python
:::

::: {.literalinclude emphasize-lines="3,7" caption="plot.py"}
tut-nwchem/plot.py
:::

Creating the Workflow
---------------------

We can write a simple script to create one instance of this workflow.
The steps include:

> -   Defining a grid of *r* values
> -   Creating a **nwchem-water** task for each r value
> -   Creating a **plot-pes** task
> -   Adding a Dependency from each **nwchem-water** parent task to the
>     **plot-pes** task

The script should look as follows. One line is highlighted for each of
the four steps listed above. You will notice that we are setting the
**data** attribute in the BalsamJob constructor for each
**nwchem-water** task. This field can hold arbitrary JSON data; it is
stored efficiently in a Postgres binary format (JSONB) and is [readily
queried with
Django](https://docs.djangoproject.com/en/2.1/ref/contrib/postgres/fields/#querying-jsonfield).

::: {.highlight}
python
:::

::: {.literalinclude emphasize-lines="6,10,24,32" caption="populate.py"}
tut-nwchem/populate.py
:::

Preprocess: Generating NWchem input
-----------------------------------

We need to create an input file for each NWChem calculation before it
runs. To do this, we can place some logic in the **preprocess** script
`pre.py`{.interpreted-text role="bash"} that is used in the
**nwchem-water** ApplicationDefinition. This script:

> -   uses `balsam.launcher.dag.current_job`{.interpreted-text
>     role="bash"} to grab the context of the current job
> -   reads the internal coordinates (*r*, *theta*) from the task
>     **data** field
> -   converts to Cartesian (*xyz*) coordinates
> -   writes-out an input deck for a Hartree Fock calculation in the
>     6-31G basis

The four steps above are mapped to the highlighted lines in the script
below:

::: {.highlight}
python
:::

::: {.literalinclude emphasize-lines="5,32-33,34,36" caption="pre.py"}
tut-nwchem/pre.py
:::

::: {.note}
::: {.title}
Note
:::

Pre- and Post-processing scripts run in the task\'s working directory,
so we don\'t have to worry about absolute paths when opening files.
:::

Postprocess: Parse and store NWChem output
------------------------------------------

The last piece of our workflow is the `post.py`{.interpreted-text
role="bash"} script responsible for collecting NWChem outputs after each
**nwchem-water** task finishes.

Note that that the STDOUT and STDERR of each task are directed to a file
named `{job.name}.out`{.interpreted-text role="bash"}. The highlighted
line in the script below shows this construction. We simply scan through
the lines of this file and extract the final HF energy. Finally, we
store it in the JSON **data** field so the **plot-pes** step can find
this result at the end.

::: {.highlight}
python
:::

::: {.literalinclude emphasize-lines="6" caption="post.py"}
tut-nwchem/post.py
:::

::: {.note}
::: {.title}
Note
:::

Post-processing scripts can also be used to programatically handle
errors. This is enabled by setting the **post\_error\_handler** flag in
the BalsamJob, and inspecting the `current_job.state`{.interpreted-text
role="bash"} in the postprocessor. In this tutorial, the postprocess is
only invoked after successful completion of runs.
:::

Putting it All Together
-----------------------

Now we can populate the DB with our ApplicationDefinitions and workflow,
then submit a job and watch it go. We need to be sure that our scripts
are executable, since we used the *\"script.py\"* convention instead of
*\"/usr/env/bin python script.py\"*.

::: {.highlight}
bash
:::

``` {.bash}
$ chmod +x *.py  # set exe permission
$ python apps.py # populate apps
$ balsam ls apps --verbose # check apps in DB

$ python populate.py
$ balsam ls # check jobs in DB

$ balsam submit-launch -n 5 -t 60 -A Project -q Queue --job-mode=mpi
```

When the workflow starts running, the Balsam command line is a great way
to quickly navigate the tasks and see the status of everything in
realtime. Follow along to learn some of the neat navigation tricks
below:

::: {.highlight}
bash
:::

``` {.bash}
# count up jobs by state
$ balsam ls --by-states 
JOB_FINISHED  10
RUNNING  5
PREPROCESSED  85
AWAITING_PARENTS  1


# list finished jobs only
$  balsam ls --state JOB_FINISHED 

                            job_id |   name | workflow |  application |        state
--------------------------------------------------------------------------------------
159070cb-03f8-4a37-9ed1-66bfb906e4bb | task4  | demo     | nwchem-water | JOB_FINISHED
db12a2e7-f8b1-4eb5-9c03-5bb0c4ac74f8 | task3  | demo     | nwchem-water | JOB_FINISHED
72d7f99e-76a5-495d-9d5f-648e0582e3a4 | task2  | demo     | nwchem-water | JOB_FINISHED


# change directory (cd) to task with job_id starting with 1590
$ . bcd 1590  
$ ls
h2o.movecs  h2o.p  h2o.zmat  input.nw  postprocess.log  preprocess.log  task4.out

# This one is cool: use BALSAM_LS_FIELDS to add "data" to the table display
# Then list finished jobs to see the results at a glance!
$  BALSAM_LS_FIELDS=data balsam ls --state JOB_FINISHED
                            job_id |   name | workflow |  application |        state |                                                                  data
--------------------------------------------------------------------------------------------------------------------------------------------------------------
159070cb-03f8-4a37-9ed1-66bfb906e4bb | task4  | demo     | nwchem-water | JOB_FINISHED | {'r': 0.8408163265306123, 'theta': 104.5, 'energy': -75.949402426371}
db12a2e7-f8b1-4eb5-9c03-5bb0c4ac74f8 | task3  | demo     | nwchem-water | JOB_FINISHED | {'r': 0.8306122448979593, 'theta': 104.5, 'energy': -75.941790242862}
72d7f99e-76a5-495d-9d5f-648e0582e3a4 | task2  | demo     | nwchem-water | JOB_FINISHED | {'r': 0.8204081632653062, 'theta': 104.5, 'energy': -75.93319131096}
e667e9e8-b77d-4e78-9b73-f5084cc62e3a | task7  | demo     | nwchem-water | JOB_FINISHED | {'r': 0.8714285714285714, 'theta': 104.5, 'energy': -75.966950538242}
77ecaf92-e462-49b2-8823-2d5a9c449528 | task49 | demo     | nwchem-water | JOB_FINISHED | {'r': 1.3, 'theta': 104.5, 'energy': -75.863011732378}
6ca50987-d509-43aa-9604-da1d0d625a82 | task3  | demo     | nwchem-water | JOB_FINISHED | {'r': 0.8306122448979593, 'theta': 104.5, 'energy': -75.941790242862}
46f689e1-7a1a-49b7-bc74-4d058b6faa1b | task19 | demo     | nwchem-water | JOB_FINISHED | {'r': 0.9938775510204082, 'theta': 104.5, 'energy': -75.981078202929}
b83a6cb2-5ad9-44aa-b28b-d665bbbfbedf | task10 | demo     | nwchem-water | JOB_FINISHED | {'r': 0.9020408163265307, 'theta': 104.5, 'energy': -75.977730477137}
b618dcbf-1e49-46e9-9b2f-b0ee69904dfc | task1  | demo     | nwchem-water | JOB_FINISHED | {'r': 0.8102040816326531, 'theta': 104.5, 'energy': -75.923535853437}
ac765c84-6544-4bf0-9f60-b898cb6ec117 | task2  | demo     | nwchem-water | JOB_FINISHED | {'r': 0.8204081632653062, 'theta': 104.5, 'energy': -75.93319131096}

# Check on the Plot step
$ balsam ls --name plot
                            job_id | name | workflow | application |   state
------------------------------------------------------------------------------
fce9ecb3-b025-4e18-b499-a5c21e97e4ee | plot | demo     | plot-pes    | JOB_FINISHED

# Change to the plot dir
$ . bcd fce9
$ cat plot.out
```
