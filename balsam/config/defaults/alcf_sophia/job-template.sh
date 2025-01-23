#!/bin/bash
#PBS -l select={{ num_nodes }}
#PBS -l walltime={{ wall_time_min//60 | int }}:{{ wall_time_min | int }}:00
#PBS -l filesystems=home:grand:eagle
#PBS -A {{ project }}
#PBS -q {{ queue }}

export http_proxy="http://proxy:3128"
export https_proxy="http://proxy:3128"

export BALSAM_SITE_PATH={{balsam_site_path}}
cd $BALSAM_SITE_PATH

module load conda
conda activate base

echo "Starting balsam launcher at $(date)"
{{launcher_cmd}} -j {{job_mode}} -t {{wall_time_min - 2}}  \
{% for k, v in filter_tags.items() %} --tag {{k}}={{v}} {% endfor %} \
{{partitions}}
echo "Balsam launcher done at $(date)"
