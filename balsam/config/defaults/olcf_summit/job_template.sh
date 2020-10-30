#!/bin/bash
#BSUB -nnodes {{ num_nodes }}
#BSUB -W {{ wall_time_min }}
#BSUB -P {{ project }}


{{ balsam_bin }}/balsam launcher --{{ wf_filter }} --job-mode={{ job_mode }} --time-limit-minutes={{ wall_time_min-2 }}
