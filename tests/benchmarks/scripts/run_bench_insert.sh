#!/bin/bash -x
#COBALT -A datascience
#COBALT -n 1024
#COBALT -q default
#COBALT -t 45
#COBALT -M msalim@anl.gov

source ~/.bash_profile
source activate balsam

cat ~/hpc-edge-service/testdb/dbwriter_address
rm ~/hpc-edge-service/testdb/log/*.log

export BALSAM_DB_PATH=~/hpc-edge-service/testdb

balsam-test tests.benchmarks.bench_insertion
