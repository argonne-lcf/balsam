#!/bin/bash
#SBATCH --nodes={{ num_nodes }}
#SBATCH --time={{ time_minutes }}
#SBATCH --qos={{ queue }}
#SBATCH --account={{ project }}
#SBATCH --constraint=knl


{{ balsam_bin }}/balsam launcher --{{ wf_filter }} --job-mode={{ job_mode }} --time-limit-minutes={{ time_minutes-2 }}