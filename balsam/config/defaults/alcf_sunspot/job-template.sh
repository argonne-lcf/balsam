#!/bin/bash
#PBS -l select={{ num_nodes }}:system=sunspot,place=scatter
#PBS -l walltime={{ wall_time_min//60 | int }}:{{ wall_time_min | int }}:00
#PBS -l filesystems=home
#PBS -A {{ project }}
#PBS -q {{ queue }}

export HTTP_PROXY=http://proxy.alcf.anl.gov:3128
export HTTPS_PROXY=http://proxy.alcf.anl.gov:3128
export http_proxy=http://proxy.alcf.anl.gov:3128
export https_proxy=http://proxy.alcf.anl.gov:3128

#remove export PMI_NO_FORK=1
export BALSAM_SITE_PATH={{balsam_site_path}}
cd $BALSAM_SITE_PATH

echo "Starting balsam launcher at $(date)"
{{launcher_cmd}} -j {{job_mode}} -t {{wall_time_min - 2}}  \
{% for k, v in filter_tags.items() %} --tag {{k}}={{v}} {% endfor %} \
{{partitions}}
echo "Balsam launcher done at $(date)"
