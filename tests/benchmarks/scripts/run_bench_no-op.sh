#!/bin/bash -x
#COBALT -A datascience
#COBALT -n 3
#COBALT -q debug-flat-quad
#COBALT -t 25
#COBALT -M msalim@anl.gov

source ~/.bash_profile
source activate balsam

rm ~/testdb/log/*.log

export BALSAM_DB_PATH=~/testdb

balsam-test tests.benchmarks.bench_no_op
