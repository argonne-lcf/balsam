---
hide:
  - toc
---

# Installation

Balsam requires Python3.7+ and is tested on Linux and MacOS.
Within any suitable Python environment, Balsam can be installed using `pip`:

```bash
# Use --pre to get the Balsam pre-release
$ pip install --pre balsam-flow 
```

Balsam developers or service administrators should instead follow the [developer installation instructions](../development/contributing.md).

## Supported Sites

Balsam is easily extensible to new HPC systems. Default configurations are available for the following systems:

| Facility | System | Configuration Included? |
|----------|--------|:------------------------:|
| ALCF     | Theta (KNL) | :material-check-circle:  |
| ALCF     | Theta (GPU) | :material-check-circle:  |
| ALCF     | Cooley | :material-check-circle:  |
| NERSC     | Cori | :material-check-circle:  |
| OLCF     | Summit | :material-check-circle:  |
| ---     | Mac OS | :material-check-circle:  |


### Summit (OLCF)

The `cryptography` sub-dependency of `globus-sdk` can be troublesome on non-x86 environments, where 
`pip` may attempt to build  it from source.  One workaround is to create a conda environment with the 
`cryptography` dependency pre-satisfied, from which `pip install` works smoothly:

```bash
$ module load gcc/10.2.0
$ module load python/3.7.0-anaconda3-5.3.0

$ conda init bash
$ source ~/.bashrc
$ conda create -p ./b2env "cryptography>=1.8.1,<3.4.0" -y
$ conda activate ./b2env

$ pip install --pre balsam-flow
```