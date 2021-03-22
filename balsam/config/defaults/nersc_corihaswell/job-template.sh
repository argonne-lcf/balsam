#!/bin/bash
#SBATCH --nodes={{ num_nodes }}
#SBATCH --time={{ wall_time_min }}
#SBATCH --qos={{ queue }}
#SBATCH --account={{ project }}
#SBATCH --constraint=haswell
{% if optional_params.get("reservation") %}#SBATCH --reservation={{optional_params["reservation"]}} {% endif %}


export PMI_NO_FORK=1
export BALSAM_SITE_PATH={{balsam_site_path}}
cd $BALSAM_SITE_PATH

echo "Starting balsam launcher at $(date)"
{{launcher_cmd}} -j {{job_mode}} -t {{wall_time_min}}  \
{% for k, v in filter_tags.items() %} --tag {{k}}={{v}} {% endfor %} \
{{partitions}}
echo "Balsam launcher done at $(date)"
