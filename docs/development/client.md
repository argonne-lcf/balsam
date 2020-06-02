# The API Client

In transitioning Balsam from a **database-driven** application to a multi-user, **Web
client-driven** application, we have had to rethink how the Python API
should look. Both internal Balsam components and user-written scripts need a way to 
manipulate and synchronize with the central state.

In Balsam `0.x`, users leverage direct access to the Django ORM and manipulate
the database with simple APIs like:
`BalsamJob.objects.filter(state="FAILED").delete()`.
Obviously, direct database access is not acceptable in a multi-user application.
However, in cutting off access to the Django ORM, users would lose the familiar API
(arguably one of Balsams' most important features) and have to drop down to writing
and decoding JSON data for each request.

The client architecure described below provides a solution to this problem with
a *Django ORM-inspired* API.  A familiar Python object model of the data,
complete with models (e.g. `Job`), managers (`Job.objects`), and Querysets
(`Job.objects.filter(state="FAILED").delete()`) is available. Instead of
accessing a database, execution of these "queries" results in a REST API call.

Internally, a `RESTClient` interface encapsulates the HTTP request and authentication logic and
contains `Resource` components that map ordinary Python methods to API methods.

![Client Architecture](../graphs/client.png)

## The `RESTClient` interface

All Python interactions with the Balsam REST API occur through the
`balsam.client.RESTClient` interface.  The base `RESTClient` is a
composite class containing several `Resource` components that comprise the API's endpoints:

  1. `users`
  2. `sites`
  3. `apps`
  4. `batch_jobs`
  5. `jobs`
  6. `events`
  7. `sessions`

In addition to these `Resources`, the RESTClient provides interfaces to 
perform the actual requests and authenticate to the API:

```
class RESTClient:
    def interactive_login(self):
        """Initiate interactive login flow"""

    def refresh_auth(self):
        """
        Reload credentials if stored/not expired.
        Set appropriate Auth headers on HTTP session.
        """

    def request(self, absolute_url, http_method, payload=None):
        """
        Perform API request and return response data
        Supports timeout retry, auto re-authentication, accepting DUPLICATE status
        Raises helpful errors on 4**, 5**, TimeoutErrors, AuthErrors
        """
```

Subclasses of the `RESTClient` interface are responsible for these
implementations. For instance, the `DirectAPIClient` makes a direct
connection to a PostgreSQL Balsam database and passes request objects
directly into the API views.  This permits users who wish to control 
their own Balsam database to do so without running an actual web server.

The `RequestsClient` uses the Python `requests` library to perform
real HTTPS requests. It supports sessions with persistent connections
and auto-retry of timed-out requests. Subclasses of `RequestsClient`
can override `refresh_auth()` and `interactive_login()` to support
different authentication schemes.


## The `Resource` components

The base `RESTClient` implementation contains 7 `Resource` components:
one for each path or logical resource in the API.
Each `Resource`, in turn, contains a set of API action
methods that build the URL and request payload, then invoke the
`RESTClient.request()` method to perform the communication. The
generic actions look as follows:

```
class Resource:
    def list(self, **query_params):
        """Returns a collection of items matching query"""
        url = self.client.build_url(self.collection_path, **query_params)
        response = self.client.request(url, "GET")
        return self.client.extract_data(response)

    def detail(self, uri, **query_params):
        """Retrieve one object from collection"""

    def create(self, **payload):
        """Create new item; returns ID"""

    def update(self, uri, payload, partial=False, **query_params):
        """Update an exisitng item"""

    def bulk_create(self, list_payload):
        """Bulk create list of items"""

    def bulk_update_query(self, patch, **query_params):
        """Apply same patch to every item matched by query"""

    def bulk_update_patch(self, patch_list):
        """Applies a list of patches item-wise"""

    def destroy(self, uri, **query_params):
        """Delete the item given by the uri"""

    def bulk_destroy(self, **query_params):
        """Delete the items matched by the query"""
```

The `Resources` contain a circular reference back to their parent `RESTClient` to use the helper
methods `build_url` and `extract_data` as well as invoke `request`.  The uniform, flat structure
of the REST API allows the simple base `Resource` to be used for most API actions. For resources
with specialized API calls, `Resource` is be subclassed to provide the appropriate Python methods.

For example, `client.jobs` is a derived class named `JobResource` providing the additional method
`history()`, so that users may fetch all the events for a particular job via `client.jobs.history(uri)`.
A call to `client.jobs.history(3)` results in `HTTP GET` request to the path `jobs/3/events`.
