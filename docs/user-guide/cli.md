# The Command Line Interface (CLI)

When installed, Balsam provides a `balsam` command line tool for convenient,
shell-based management of your Sites, Apps, Jobs, and BatchJobs.
The CLI comprises recursively-documented subcommands: use the `--help` flag at 
any level to view example usage:

```bash
# See all commands:
$ balsam --help

# See all job-related commands:
$ balsam job --help

# See details of CLI job creation:
$ balsam job create --help
```


## The Site Selector

A key feature of the CLI (and underlying API) is that it works **across sites**:
you might, for example, query and submit jobs to three HPC facilities from a
single shell running on your laptop. This raises a namespacing concern: how
should the CLI commands target different Sites?

To answer this question, the Balsam CLI is somewhat context-aware.  When you are logged into the machine and *inside* of a Site directory, the **current site** is automatically inferred.  Thus commands like `balsam job ls` will filter the visible Jobs to only those in the current Site. Likewise, commands like  `balsam job create` or `balsam queue submit` will infer the ID of the Site for which you are trying to add a Job or BatchJob.

If you are *outside* of a Site directory, the CLI instead limits queries to all currently *active* Sites.  For example, `balsam job ls` will list Jobs across all your Sites that have an Agent process currently running.

We can override this context-dependent behavior by explicitly passing the `--site` selector argument as follows:

```bash
$ balsam job ls --site=all # Show jobs at ALL sites
$ balsam job ls --site=this # Only show jobs at the current site
$ balsam job ls --site=active # Show jobs at active sites only
```

Moreover, the `--site` selector can provide a *comma-separated list* of Site IDs or Site Path fragments:

```bash
$ balsam job ls --site=123,125
$ balsam job ls --site=myFolder/siteX,siteY,123
```

When you are *creating* a Job or enqueueing a BatchJob, the `--site` selector
must unambiguously narrow down to a single Site. In this case, use a single
numeric Site ID (as shown in `balsam site ls`) or a unique substring of the Site
path.

```bash
$ balsam queue submit --site=123 # ...
$ balsam queue submit --site=unique_name # ...
```