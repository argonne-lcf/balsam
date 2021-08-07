#!/bin/bash -lx
#COBALT -A {{ project }}
#COBALT -n {{ num_nodes }}
#COBALT -q {{ queue }}
#COBALT -t {{ wall_time_min }}
#COBALT --attrs pubnet

# Uncomment this if the server is on an external network
# (Note that https_proxy is set to use an `http://` protocol!
# Do not set other proxy env vars):
# export https_proxy=http://theta-proxy.tmi.alcf.anl.gov:3128

export PMI_NO_FORK=1

export BALSAM_SITE_PATH={{balsam_site_path}}
cd $BALSAM_SITE_PATH

echo "Starting balsam launcher at $(date)"
{{launcher_cmd}} -j {{job_mode}} -t {{wall_time_min}}  \
{% for k, v in filter_tags.items() %} --tag {{k}}={{v}} {% endfor %} \
{{partitions}}
echo "Balsam launcher done at $(date)"
