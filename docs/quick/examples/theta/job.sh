#!/bin/bash -x
#COBALT -A Performance
#COBALT -n 2
#COBALT -t 30
#COBALT -q debug-cache-quad

#
# See docs/quickstart.rst for how to setup and run on Theta
#

module load intelpython35
source activate balsam

balsam ls | grep "^No"
if [ $? -eq 0 ];
then
  echo "Add jobs to balsam first with balsam qsub"
  exit 1
fi

balsam ls
balsam launcher --consume-all \
                --num-workers 2 \
                --nodes-per-worker 1 \
                --max-ranks-per-node 64 
