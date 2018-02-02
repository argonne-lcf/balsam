#!/bin/bash -x
#COBALT -A datascience
#COBALT -n 4
#COBALT -q debug-cache-quad
#COBALT -t 10
#COBALT -M msalim@anl.gov
#COBALT --cwd ~/hpc-edge-service

source ~/.bash_profile
source activate balsam

cat testdb/dbwriter_address
rm testdb/log/*.log

export BALSAM_DB_PATH=~/hpc-edge-service/testdb

balsam-test tests.benchmarks.bench_insertion
