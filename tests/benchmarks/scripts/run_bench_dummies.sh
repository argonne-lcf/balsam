#!/bin/bash -x
#COBALT -A datascience
#COBALT -n 8
#COBALT -q debug-cache-quad
#COBALT -t 15
#COBALT -M msalim@anl.gov

source ~/.bash_profile
source activate balsam

rm ~/testdb/log/*.log

export BALSAM_DB_PATH=~/testdb # postgres DB server must be active here!

aprun -n 8 -N 1 balsam make_dummies 1
time aprun -n 100 -N 13 balsam make_dummies 100
time aprun -n 100 -N 13 balsam make_dummies 100
time aprun -n 100 -N 13 balsam make_dummies 100
