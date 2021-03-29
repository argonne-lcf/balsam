#!/bin/bash -x
#COBALT -A {{ project }}
#COBALT -n {{ num_nodes }}
#COBALT -q {{ queue }}
#COBALT -t {{ wall_time_min }}
#COBALT --attrs ssds=required:ssd_size=128


{% if optional_params.get("singularity_prime_cache") %}
aprun -N 1 -n $COBALT_JOBSIZE /soft/tools/prime-cache
sleep 10
{% endif %}

# Uncomment this if the server is on an external network
# (Note that https_proxy is set to use an `http://` protocol!
# Do not set other proxy env vars):
export https_proxy=http://theta-proxy.tmi.alcf.anl.gov:3128

module unload trackdeps
module unload darshan
module unload xalt
export PMI_NO_FORK=1

export BALSAM_SITE_PATH={{balsam_site_path}}
cd $BALSAM_SITE_PATH

PY_EXE=/home/msalim/balsam/envs/balsam/bin/python
# L 

$PY_EXE -t 10 -n 32 -s L -e eig-scaling-theta-4 >& 32L.log &
{{launcher_cmd}} -j mpi -t 17  --part mpi:32:num_nodes=32:size=L:experiment=eig-scaling-theta4
echo "Balsam launcher (32) done at $(date)"
wait

$PY_EXE -t 10 -n 16 -s L -e eig-scaling-theta-4 >& 16L.log &
{{launcher_cmd}} -j mpi -t 17  --part mpi:16:num_nodes=16:size=L:experiment=eig-scaling-theta4
echo "Balsam launcher (16) done at $(date)"
wait

$PY_EXE -t 10 -n 8 -s L -e eig-scaling-theta-4 >& 8L.log &
{{launcher_cmd}} -j mpi -t 17  --part mpi:8:num_nodes=8:size=L:experiment=eig-scaling-theta4
echo "Balsam launcher (8) done at $(date)"
wait

$PY_EXE -t 10 -n 4 -s L -e eig-scaling-theta-4 >& 4L.log &
{{launcher_cmd}} -j mpi -t 17  --part mpi:4:num_nodes=4:size=L:experiment=eig-scaling-theta4
echo "Balsam launcher (4) done at $(date)"
wait

# C

$PY_EXE -t 10 -n 32 -s C -e eig-scaling-theta-4 >& 32C.log &
{{launcher_cmd}} -j mpi -t 17  --part mpi:32:num_nodes=32:size=C:experiment=eig-scaling-theta4
echo "Balsam launcher (32C) done at $(date)"
wait

$PY_EXE -t 10 -n 16 -s C -e eig-scaling-theta-4 >& 16C.log &
{{launcher_cmd}} -j mpi -t 17  --part mpi:16:num_nodes=16:size=C:experiment=eig-scaling-theta4
echo "Balsam launcher (16C) done at $(date)"
wait

$PY_EXE -t 10 -n 8 -s C -e eig-scaling-theta-4 >& 8C.log &
{{launcher_cmd}} -j mpi -t 17  --part mpi:8:num_nodes=8:size=C:experiment=eig-scaling-theta4
echo "Balsam launcher (8C) done at $(date)"
wait

$PY_EXE -t 10 -n 4 -s C -e eig-scaling-theta-4 >& 4C.log &
{{launcher_cmd}} -j mpi -t 17  --part mpi:4:num_nodes=4:size=C:experiment=eig-scaling-theta4
echo "Balsam launcher (4C) done at $(date)"
wait
