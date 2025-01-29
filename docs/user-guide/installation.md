---
hide:
  - toc
---

# Installation

Balsam requires Python3.7+ and is tested on Linux and MacOS.
Within any suitable Python environment, Balsam can be installed using `pip`:

```bash
# Use --pre to get the Balsam pre-release
$ pip install --pre balsam
```

Balsam developers or service administrators should instead follow the [developer installation instructions](../development/contributing.md).

## Supported Sites

Balsam is easily extensible to new HPC systems. Default configurations are available for the following systems:

| Facility | System | Configuration Included? |
|----------|--------|:------------------------:|
| ALCF     | Aurora | :material-check-circle:  |
| ALCF     | Polaris | :material-check-circle:  |
| ALCF     | Sunspot | :material-check-circle:  |
| NERSC     | Perlmutter-CPU | :material-check-circle:  |
| NERSC     | Perlmutter-GPU | :material-check-circle:  |
| ---     | Mac OS | :material-check-circle:  |


