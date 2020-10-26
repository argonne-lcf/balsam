#!/bin/bash
#SBATCH --nodes={{ num_nodes }}
#SBATCH --time={{ wall_time_min }}
#SBATCH --qos={{ queue }}
#SBATCH --account={{ project }}
#SBATCH --constraint=knl


{{ balsam_bin }}/balsam launcher --{{ wf_filter }} --job-mode={{ job_mode }} --time-limit-minutes={{ wall_time_min-2 }}
