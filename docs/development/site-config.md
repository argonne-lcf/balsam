# Site Configuration

Several fields in the Site configuration file or Job Template are sync'ed with the `Site` API:

  - `allowed_projects` lets the API know about projects that the Site may submit to
  - `allowed_queues` lets the API know about the local queue policy
  - `transfer_locations` lets the API know about remote Globus endpoints
    or scp addresses that the Site is willing to stage data in/out from
  - `optional_batch_job_params` lets the API know about "pass-through" parameters that the Job template
     will accept in submitting a BatchJob

## The API Client configuration

## How the CLI reads configuration



## Local configuration

A Balsam Site is initialized with `balsam init`.

Site settings for each launcher Job mode dictate whether multi-apps-per-node is supported and what is the max occupancy per node.

## Global configuration
