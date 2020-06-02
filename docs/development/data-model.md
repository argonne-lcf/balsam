# Data Model and REST API

![Database Schema](../graphs/db.png)

This graph shows the Django models, or database schema, of the Balsam
application. Each node is a table in the database, represented by one
of the model classes in the Django ORM.
Each arrow represents a ForeignKey 
(or [many-to-one](https://docs.djangoproject.com/en/3.0/topics/db/examples/many_to_one/))
relationship between two tables.

## High-level view of the database

  1. A `User` represents a Balsam user account.  **All** items in the database
     are linked to a single owner, which is reflected in the connectivity of
     the graph. For example, to get all the jobs belonging to `current_user`,
     join the tables via `Job.objects.filter(app__site__user=current_user)`

  2. A `Site` is uniquely identified as a directory on some machine: `(machine
     hostname,  directory path)`. One user can own several Balsam sites
     located across one or several machines.  Each site is an *independent*
     endpoint where applications are registered, data is transferred in and
     out, and Job working directories are located. A Balsam service daemon, which is 
     **authenticated** to the REST API, is uniquely associated with a single `Site` and
     runs as the user on that system. If a user has multiple active Balsam Sites, then a separate
     service runs at each of them.  The authenticated daemon communicates with the central
     Balsam API to fetch jobs, orchestrate the workflow locally, and update the database state. 

  3. An `App` represents a runnable application at a particular Balsam Site.
     Every Balsam Site contains an `apps/` directory with Python modules
     containing `ApplicationDefinition` classes.  The set of
     `ApplicationDefinitions` determines the applications which may run at the
     Site.  An `App` instance in the data model is merely a reference to an
     `ApplicationDefinition` class, uniquely identified by the Site ID and
     class path. 

  4. A `Job` represents a single run of an `App` at a particular `Site`.  The
     `Job` contains both application-specific data (like command line
     arguments) and resource requirements (like number of MPI ranks per node)
     for the run. It is important to note that Job-->App-->Site are
     non-nullable relations, so a `Job` is always bound
     to run at a particular `Site`.  Therefore, the corresponding Balsam service
     daemon may begin staging-in data as soon as a `Job` becomes visible, as appropriate.

  5. A `BatchJob` represents a job launch script and resource request submitted
     by the `Site` to the local workload manager or job Scheduler. Notice that
     the relation of `BatchJob` to `Site` is many-to-one, and that `Job` to
     `BatchJob` is many-to-one. That is, many `Jobs` run in a single
     `BatchJob`, and many `BatchJobs` are submitted at a `Site` over time.

  6. The `Lock` is an internal model representing an active Balsam launcher
     session.  `Jobs` have a nullable relationship to `Lock`; when it is not
     null, the job is said to be *locked* by a launcher, and no other launcher
     should try running it.  The Balsam **session API** is used by launchers to
     acquire jobs concurrently and without race conditions.  Locks contain a
     heartbeat timestamp that must be periodically ticked to maintain the lock.

   7. A `TransferItem` is created for each stage-in or stage-out task
      associated with a `Job`. This permits the transfer module of the Balsam
      service to group transfers according to the remote source or destination,
      and therefore batch small transfers efficiently. When all the stage-in `TransferItems`
      linked to a `Job` are finished, it is considered "staged-in" and moves ahead to
      preprocessing.

   8. A `LogEvent` contains a `timestamp`, `from_state`, `to_state`, and `message` for each
   state transition linked to a `Job`.  The benefit of breaking a Job's state history out into
   a separate Table is that it becomes easy to query for aggregate throughput, etc... without
   having to first parse and accumulate timestamps nested inside a `Job` field.


## User API

The `User` model Extends Django's [default User
model](https://docs.djangoproject.com/en/3.0/ref/contrib/auth/) via
`AbstractUser`. It contains fields like `username` and `email` but is loosely
coupled to the Authentication scheme.

Django REST Framework easily permits swapping Authentication backends or using multiple
authentication schemes.  For every view requiring authentication, 
a `user` object is available on the `request` instance, containing the pre-authenticated
User.

Generally, Balsam will need two types of Auth to function:

1. **Login auth:** This will likely be an pair of views providing an
   OAuth flow, where Balsam redirects the user to an external auth system,
   and upon successful authentication, user information is redirected *back*
   to a Balsam callback view. For testing purposes, basic password-based login
   could be used instead.
2. **Token auth:** After the initial login, Balsam clients need a way to
   authenticate subsequent requests to the API.  This can be performed with
   Token authentication and a secure setup like [Django REST
   Knox](https://github.com/James1345/django-rest-knox).  Upon successful
   *login* authentication (step 1), a Token is generated and stored (encrypted)
   for the User.  This token is returned to the client in the login response.
   The client then stores this token, which has some expiration date, and
   includes it as a HTTP header on every subsequent request to the API (e.g.
   `Authorization: Token 4789ac8372...`). This is both how Javascript web clients and automated Balsam Site services can communicate with the API.


## Site

### Fields

| Field Name  | Description |
| -----------  | ----------- |
| `id ` | Unique Site ID | 
| `hostname ` | The server address or hostname like `thetalogin3.theta.alcf.anl.gov` |
| `path ` | Absolute POSIX path to the Site directory |
| `last_refresh ` | Automatically updated timestamp: last update to Site information |
| `creation_date ` | Timestamp when Site was created             |
| `owner ` | ForeignKey to `User` model             |
| `globus_endpoint_id ` | Optional `UUID`: setting an associated endpoint for data transfer |
| `num_nodes`  | Number of compute nodes available at the Site           |
| `num_idle_nodes`  |  Number of currently idle nodes        | 
| `num_busy_nodes`  | Number of currently busy nodes         |
| `backfill_windows`  | JSONField: array of `[queue, num_nodes, wall_time_min]` tuples indicating backfill slots |
