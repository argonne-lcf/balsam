The Balsam Database
===================

Every Balsam instance is associated with a persistent **database**
directory, which is created with the
`balsam init <db-path>` [command line interface](cli.md).
You will find this contains
the following:

-   **balsamdb/**: The actual PostgreSQL database directory. The
    *balsamdb/log/* directory contains detailed logs of Balsam client
    connections and queries.
    The `balsamdb/postgresql.conf` contains adjustable PostgreSQL
    configuration. This is automatically set up during
    `balsam init` and a port is automatically assigned when starting up 
    a new server process with `balsamactivate`. 
    You probably never need to look here.
-   **data/**: By default, each task working directory is created
    here under the subdirectory `data/{workflow}/{name}_{id}`. 
    The output of each Balsam task is written to its working directory.
    Refer to [Balsam job fields](app.md) for details on how to set
    your own working directory if necessary.
-   **log/**: A separate log file is created by each launcher instance
    and the Balsam service. There will be detailed log messages for
    every task launch and error that occurs in the course of your
    workflow. Look here or stream logs in real-time with
    `balsam log` to follow execution closely.
-   **qsubmit/**: this directory stores every launcher job script that
    is submitted to the local batch scheduler. The output and
    scheduler logs from each batch submission is written here as well. 
-   **server-info**: JSON-encoded data that
    tells Balsam components what host and port the database server is
    running on.

!!! warning
    If you ever move your database around on the file system or adjust
    permissions, keep in mind that the permissions on the **balsamdb/**
    subdirectory **must** remain as 700
    (`chmod balsamdb/ 700`). For security
    reasons, PostgreSQL will refuse to start if this folder has permissions
    set differently. The other directories can easily be shared with
    collaborators in your UNIX group.

Starting up the Balsam database Server
--------------------------------------

To point your current terminal session at a particular Balsam DB, use:

``` {.bash}
$ . balsamactivate <db-path or name>
```

This will a start a server process on the current node, if one isn't
already running yet. Otherwise, it will connect to the existing server
referenced in the **server-info**. The Postgres server stays alive even
when you log off, so subsequent jobs and CLI sessions will typically
re-connect to the same server until it is explicitly shut down or the host is 
rebooted.

!!! note
    The `balsamactivate` line is automatically included in each
    launcher job, as you will see in the **qsubmit/** job scripts. This
    helps to ensure that jobs start up successfully, even if the server was
    killed while the job was waiting in the queue

Switching Databases
-------------------

You can also use `balsamactivate` to switch context between
different databases. The command prompt decorator should always show you
which database the environment is currently pointing at. Moreover, the
command `balsam which`{.interpreted-text role="bash"} will provide
detailed information on the current active DB.

If you want to kill a DB server, first check what node it\'s running on
with `balsam which` and log into that node. You can then
identify the parent server process and kill it gracefully by sending
SIGTERM:

```.bash
[BalsamDB: foo] $  ps aux | grep $USER | grep postgres
user  39608  0.0  0.0  6495720    980   ??  Ss    4:29PM   0:00.00 postgres: checkpointer process
user  39606  0.0  0.0  4321984    820   ??  Ss    4:29PM   0:00.00 postgres: logger process
user  39605  0.0  0.4  6496000  72072 s002  S     4:29PM   0:00.06 /path/to/bin/postgres -D /home/user/foo/balsamdb

[BalsamDB: foo] $ kill 39605 
```
