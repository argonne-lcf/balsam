Multi-user collaboration with Balsam
=====================================

The PostgreSQL database underlying Balsam has strong support for concurrency
and enables multi-user workflows. You can initialize a shared DB and authorize
others to connect, view and manipulate tasks, and launch their own jobs. This
is a powerful way for collaborators to centralize the workload in a
high-throughput computing project and distribute computation across batch jobs
belonging to different users.

Balsam supports simultaneous launcher jobs and has shown near 100% utilization
across 5 concurrent jobs occupying 4010 Theta compute nodes at the ALCF. You
can provide `workflow` tags to control which tasks run in a given job,
or simply let each launcher job consume all possible work from the database.
As always, Balsam tracks the status of all tasks and batch jobs under the project
to facilitate debugging and analysis of project utilization and throughput over time.

Just a few steps need to be taken by the owner of the Balsam database to grant
Postgres permissions and ensure file permissions are also set correctly.

Multi-user setup
-----------------------------------------------

First, navigate to a directory belonging to your UNIX group (somewhere inside
`/projects/` on Theta) and initialize a Balsam DB. By placing the Balsam DB
here and setting group permissions correctly, you ensure that other members of
the project group can access your Balsam database.

Suppose you want to create a shared Balsam DB in the current directory named
*my-balsam-project*. Group permissions should be set immediately after creating
the DB as follows::

    balsam init my-balsam-project
    find my-balsam-project/ -type d -exec chmod g+rwx {} \;
    find my-balsam-project/ -type f -exec chmod  g+rw {}   \;
    find my-balsam-project/ -executable -type f -exec chmod g+x {} \;
    chmod 700 my-balsam-project/balsamdb  # otherwise, postgres will REFUSE to start !!

Next, activate the Balsam DB and authorize users to connect as follows:: 

    . balsamactivate my-balsam-project
    balsam server --add-user <collaborator_username>

.. warning:: Only the Balsam DB owner can start or kill the server process!
    Collaborators must use `source balsamactivate` only to connect to an
    **existing** Balsam database server.  This means if you intend for others to
    work with your Balsam DB, you must call `balsamactivate` first, and ensure
    that the server is left alive. Simply logging off Theta after sourcing
    `balsamactivate` will keep the server alive and is sufficient.

Activate Balsam DB and set `umask`
----------------------------------------------

Once authorized, all users can simply `source balsamactivate`, set `umask`, and
begin working. **It is essential to set umask so that directories created
during your Balsam runs are writeable by other group members. If
you forget this, your collaborators will experience Permission Denied
errors!**

The following lines should be called both in interactive Balsam sessions and in
your launcher job scripts. **If you intend to collaborate on the same tasks,
be sure to add the umask line to the templates in your** `~/.balsam/job-templates`
**directory!** This will ensure that all launcher jobs inherit the correct umask.::

    source balsamactivate my-balsam-project
    umask g=rwx

Copy or Move a Balsam DB 
--------------------------
The entire database directory structure can be transferred to another location
or filesystem. Balsam should continue to work normally with the copied
database, as long as file permissions are set correctly: the `balsamdb/`
directory must still have *700* permissions and group permissions should be set
correctly for everything else.
