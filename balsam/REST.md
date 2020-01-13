Filter, Sort, Paginate
----------------------
All collection views should take query parameters for:
    - sort (order by arbitrary fields)
    - filters (can mostly use Django-Filter backend; for JSON fields (Job tags & data), add custom query handler
    - field selectors
    - limit (impose a max limit to avoid DenialOfService)
    - offset
    - Representation should include start_idx and total_count (so Client knows its viewing 30-40 out of 42; for example)
    - Give all optional parameters sensible defaults
        - e.g. limit 100 & offset 0
        - sort by most-recently-modified
        - retrieve all fields

HTTP semantics & Return codes
---------------------------------

Avoid chatty APIs: bulk create, update, acquire
Avoid extraneous fetching: pagination and nesting URIs instead of full representations 

401: Unauthorized (tell client to authenticate via WWW-Authenticate header)
403: Permission Denied (client is authenticated; but tried to access a resource without permissions)
GET: 
    returns 200 (OK) or 404 (not found)
POST:
    400 (Bad Request): response body should contain error message
    201 (Created) with URI of new resource in "Location" header
    Can return 200 (OK) if no new object was created, with result in response body
    Return 204 (No content) if there is no response body
PUT/PATCH:
    201 (Created)
    200 (OK)
    204 (No Content)
    409 (Conflict)
DELETE:
    204 (No content)
    404 (not found)
    409 (Conflict; cannot delete)

Hyperlink to nested & parent resources
    -Job should hyperlink to App (parent), History (children), Transfers (children)
HATEOAS: GET requests should include a "links" array of links to related resources:
    {"rel": "app", "href": {uri}, "action": "GET"}

API versioning:
    put entire application under /api/v1/ namespace
    Then, if Schema has to change, can start serving /api/v2/ in parallel without breaking v1 clients

Auth
-----
Keep auth completely isolated from application logic: easily pluggable
First prototype: 
    Knox login takes Basic Auth, returns a token
    All other views use Session & Knox Token auth
    Tokens expire in 7 days
    Both Web site and python clients can print warning banners: expiring in 48 hours
/register
/login
/logout

Users
------
For most resources, User is not part of URI, because User object is implicit in Auth
User representation contains URIs to owned & authorized sites

/users
    GET: (admin only) see User collection
/users/{uid}
    GET: (self or admin) see User
    PUT: update User profile

Site
------
representation: 
    Host, Path
    Heartbeat, Authorized Users
    State: up or down?
    URI to Status (one-to-one relation)
        Num nodes, num idle, num busy, backfill windows, queue state
    URI to policy (one-to-one relation)
    URIs to collections: 
        Apps, EventLogs, Jobs
    
We deliberately provide URIs to nested resources only; the Balsam clients only need to check 
(host, path) for consistency.  Clients must store site ID locally.
Web landing page hits /sites then /summary?sites=1,2,3 to generate the dashboard

/sites
    GET: see collection of authorized Sites
    POST: create a new Site
        input: (host, path, authorized-user-emails)
        return: newly created Site representation, with URI/ID of Site
    PUT/PATCH: Error (no bulk put)
    DELETE: Error (no bulk delete)

/sites/{site_id}
    GET: Site representation 
    POST: Error
    PUT/PATCH: Site path, hostname, authorized_users (PUT must be idempotent)
    DELETE: site is marked *DEACTIVATED*; hidden from all future queries

/sites/{site_id}/status
    (site-->status is one-to-one; dont need pk in URI)
    GET: node-availability & status of queues
    PUT: periodically update from service
/sites/{site_id}/policy
    (site-->policy is one-to-one; dont need pk)
    GET: current policy (periodic poll by service)
    PUT: client decides to change queueing policy
/sites/{site_id}/apps
    POST: create new app

/sites/{site_id}/jobs
    GET: get all jobs at this site
    POST: bulk-create jobs.  expects a list of job resources.
    PUT/PATCH:  bulk-update jobs (must be idempotent!)
    DELETE: bulk-delete jobs (for safety: requires query params (list of pks, etc..))
        deleted jobs are marked *archived* and hidden from all future views

/sites/{site_id}/batchjobs
    POST: create a new batchjob.

BatchJobs
-----------
/batchjobs/
    POST: create a new batch job. service will submit to the scheduler
    GET: return nested policy and URIs to batch jobs (sort,filter,paginated)
/batchjobs/{sched_id}
/batchjobs/{sched_id}/jobs
- SchedulerJob
  - List schedulerjobs across authorized_sites
  - Create new schedulerjob on an authorized site
    - track owner of schedulerjob (who created it)
  - Update & Delete schedulerjob (if owner)

Apps
-----
/apps/
- App
  - List apps across authorized_sites
  - Create new app (from site only)
  - Update&Delete app (from site only)

Jobs
------
/jobs/
/jobs/acquire
/jobs/release
/jobs/{jobid}
/jobs/{jobid}/history
/jobs/{jobid}/transfers
  - List Jobs across authorized sites
    - Filtering, sorting, pagination from query_string
  - Create job (from anywhere)
  - Update/Delete job (if owner of the job)
    - Note that site owner != job owner
  - Bulk create jobs
    - Custom Serializer
    - Bulk creation is for a single-app
    - Validate: the app exists & belongs to an authorized_site
  - Bulk update & delete jobs
    - Validate: is owner of all jobs
    - Pass a list of instance (update this list of 10 jobs)
    - Or, pass a filter query (reset all jobs matching these Tags)
  - Kill job (all tasks killed)
  - Reset job
    - Input: task name
    - The task is reset to READY; all of its children AWAITING_PARENTS

Site Statistics
---------------
/summary/?sites=1,2,3,4
    GET: 
        10 most recently changed jobs
        Last hour/day/week of EventLog
        Current and last week of BatchJobs
        (Client generates throughput and utilization)
