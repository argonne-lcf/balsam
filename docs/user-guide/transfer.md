# Data Transfers

## Background

Each Balsam `Job` may require data to be staged in prior to execution or staged
out after execution. A core feature of Balsam is to interface with services such
as [Globus Transfer](https:///www.globus.org) and automatically submit and
monitor *batched* transfer tasks between endpoints.  

This enables distributed workflows where large numbers of `Jobs` with relatively
small datasets are submitted in *real-time*: the Site manages the details of
efficient batch transfers and marks individual jobs as `STAGED_IN` as the
requisite data arrives.

To use this functionality, the first step is to [define the Transfer
Slots](./appdef.md#transfer-slots) for a given Balsam App.  We can then [submit
`Jobs` with transfer items](./jobs.md#data-transfer) that fill the required
transfer slots.  

**Be sure to read these two sections in the user guide for more information.**
The only other requirement is to configure the `transfer` plugin at the Balsam
Site and authenticate with Globus, which we explain below.

## Configuring Transfers

When using the Globus transfer interface, Balsam needs an access token to
communicate with the Globus Transfer API.  You may already have an access token
stored from a Globus CLI installation on your machine: check `globus whoami` to
see if this is the case.  Otherwise, Balsam ships with the necessary tooling and you can follow the same Globus authentication flow by running:

```bash
$ balsam site globus-login
```

Next, we configure the `transfers` section of `settings.yml`:

- `transfer_locations` should be set to a dictionary of trusted [location
aliases](./jobs.md#data-transfer). If you need to add Globus endpoints, they can
be inserted here. 
- `globus_endpoint_id` should refer to the endpoint ID of the local Site. 
- `globus_endpoint_site_path` specifies the path on the Globus endpoint, which might be different from the path used on login/compute nodes (e.g. for ALCF home filesystem, paths begin with /home/${USER}, but on the dtn_home endpoint, paths begin with /${USER}.)
- `max_concurrent_transfers` determines the maximum number of in-flight
transfer tasks, where each task manages a *batch* of files for many Jobs.
- `transfer_batch_size` determines the maximum number of transfer items per transfer task.  This should be tuned depending on your workload (a higher number makes sense to utilize available bandwidth for smaller files).
- `num_items_query_limit` determines the maximum number of transfer items considered in any single transfer task submission.
- `service_period` determines the interval (in seconds) between transfer task submissions.

Globus requires that you give Balsam consent to make transfers on your behalf; consent is granted for each endpoint that you intend to use. You can review your Globus consents [here](https://auth.globus.org/v2/web/consents). For any endpoints that you have configured above (including the globus_endpoint_id), determine the Globus endpoint id, and execute the following command:

balsam site globus-login -e ENDPOINT_ID1 -e ENDPOINT_ID2

Note that globus_endpoint_id in settings.yaml will be used to stage input data in, and to stage output data out. This endpoint id will depend on the filesystem where your site is located (e.g. at ALCF, if it's in your home directory, use alcf#dtn_home; if it's on the Eagle filesystem, use alcf#eagle_dtn). Also make sure that the path to your site is set to correspond to how it is mapped on your Globus endpoint, using the `globus_endpoint_site_path` setting above.

Once `settings.yml` has been configured appropriately, be sure to restart the Balsam Site:

```bash
$ balsam site sync
```

The Site will start issuing stage in and stage out tasks immediately and
advancing Jobs as needed.  The state of transfers can be tracked using the [Python
API](./api.md):

```python
from balsam.api import TransferItem

for item in TransferItem.objects.filter(direction="in",  state="active"):
    print(f"File {item.remote_path} is currently staging in via task ID: {item.task_id}")
```
