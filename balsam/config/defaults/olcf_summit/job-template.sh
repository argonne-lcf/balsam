#!/bin/bash
#BSUB -nnodes {{ num_nodes }}
#BSUB -W {{ wall_time_min }}
#BSUB -P {{ project }}
#BSUB -alloc_flags "smt4"
#BSUB -q {{ queue }}
{% if optional_params.get("reservation") %}#BSUB -U {{optional_params["reservation"]}} {% endif %}

# Uncomment this if the server is on an external network
# (Note that https_proxy is set to use an `http://` protocol!
# Do not set other proxy env vars):
# export https_proxy=http://<some-server>

export BALSAM_SITE_PATH={{balsam_site_path}}
cd $BALSAM_SITE_PATH

echo "Starting balsam launcher at $(date)"
{{launcher_cmd}} -j {{job_mode}} -t {{wall_time_min}}  \
{% for k, v in filter_tags.items() %} --tag {{k}}={{v}} {% endfor %} \
{{partitions}}
echo "Balsam launcher done at $(date)"