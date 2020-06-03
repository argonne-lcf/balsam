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

## The REST API

Refer to the interactive and detailed API documentation (URL query parameters, sample
request and response payloads) for every endpoint at the `/api/swagger/` endpoint:

```bash
cd balsam/server
./dev/total-reset.sh
./manage.py runserver
# Navigate to 127.0.0.1:8000/api/swagger in browser
```


The rest of this page gives a condensed overview of the API, with the aim of
explaining concepts rather than serving as a reference for actual development.

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

### Model Fields

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

### API

##### Representation
The `owner` field is excluded from the serialized representation.  Created sites
implicitly belong to the authenticated user, and a user can only view or update
sites that belong to them.

##### URLs

| HTTP Method | URL | Description | Example usage |
| ------------| ----- | ---------- | -----   |
| GET | /api/sites/ | Retrieve the current user's list of sites | A user checks their Balsam site statuses on dashboard | 
| POST | /api/sites/ | Create a new Site | `balsam init` creates a Site and stores new `id` locally |
| PUT | /api/sites/{id} | Update Site information | Service daemon syncs `backfill_windows` periodically |
| DELETE | /api/sites/{id} | Delete Site | User deletes their Site with `balsam rm site` |

## App

### Model Fields
| Field Name  | Description |
| -----------  | ----------- |
| `id ` | Unique App ID |
| `site` | Foreign Key to `Site` instance containing this App | 
| `name` | Short name identifying the app. |
| `description` | Text description (useful in generating Web forms) |
| `class_path` | Name of `ApplicationDefinition` class in the format: `{module_name}.{class_name}` |
| `parameters` | A list of command line template parameters. A list of dicts with the structure: `[ {name: str, required: bool, default: str, help: str} ]` |
| `stage_ins` | A list of stage-in slots with the structure: `[ {name: str, required: bool, dest: str} ]` |
| `stage_outs` | A list of stage-out slots with the structure: `[ {name: str, required: bool, src: str} ]` |

The `App` model is used to merely *index* the `ApplicationDefinition` classes
that a user has registered at their Balsam Sites. 

The `parameters` field represents "slots" for each adjustable command line parameter.  
For example, an `ApplicationDefinition` command template of
`"echo hello, {{first_name}}!"` would result in an `App` having the `parameters` list: 
`[ {name: "first_name", required: true, default: "", help: ""} ]`.  None of the Balsam
site components use `App.parameters` internally; the purpose of mirroring this field in 
the database is simply to facilitate Job validation and create App-tailored web forms.

Similarly, `stage_ins` and `stage_outs` mirror data on the `ApplicationDefinition` for
Job input and validation purposes only.

For security reasons, the
validation of Job input parameters takes place in the site-local `ApplicationDefinition`
module. Even if a malicious user altered the `parameters` field in the API, they would not
be able to successfully run a Job with injected parameters.

### API
##### Representation
A user only sees Apps linked to Sites which belong to them.

##### URLs
| HTTP Method | URL | Description | Example usage |
| ------------| ----- | ---------- | -----   |
| GET | /api/apps/ | Retrieve the current user's list of Apps | `balsam ls apps` shows Apps across sites |
| POST | /api/apps/ | Create a new `App` | `balsam app sync` creates new `Apps` from local `ApplicationDefinitions`  |
| PUT | /api/apps/{id} | Update `App` information | `balsam app sync` updates existing `Apps` with changes from local `ApplicationDefinitions` |
| DELETE | /api/apps/{id} | Delete `App` | User deletes an `App`; all related `Jobs` are deleted |

## Job

### Model Fields
| Field Name  | Description |
| -----------  | ----------- |
| `id ` | Unique App ID |


### API
##### Representation
A user only sees Jobs linked to Apps linked to Sites which belong to them.

##### URLs
| HTTP Method | URL | Description | Example usage |
| ------------| ----- | ---------- | -----   |
| GET | /api/jobs/ | Retrieve the current user's list of Jobs | |
| POST | /api/jobs/ | Create a new `Job` | |
| PUT | /api/jobs/{id} | Update `Job` information | |
| DELETE | /api/jobs/{id} | Delete `Job` | |
