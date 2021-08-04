# Installation

Balsam requires Python3.7+ and is tested on Linux and MacOS.
With a suitable Python environment, Balsam can be installed with `pip`:

```bash
# Use --pre to get the Balsam0.6 pre-release
$ pip install --pre balsam-flow 
```

Balsam developers or service administrators should instead follow the [developer installation instructions](../development/contributing.md).

## Theta-KNL (ALCF)

```bash
# Use --pre to get the Balsam0.6 pre-release
$ pip install --pre balsam-flow 
```

## Theta-GPU (ALCF)

## Cooley (ALCF)

## Cori (NERSC)

## Summit (OLCF)

The `cryptography` sub-dependency of `globus-sdk` can be troublesome on non-x86 environments, where 
`pip` will try to build from source.  One workaround is to create a conda environment with the 
`cryptography` dependency pre-satisfied, from which `pip install` works smoothly:

```bash
module load gcc/10.2.0
module load python/3.7.0-anaconda3-5.3.0

conda init bash
source ~/.bashrc
conda create -p ./b2env "cryptography>=1.8.1,<3.4.0" -y
conda activate ./b2env

pip install --pre balsam-flow
```