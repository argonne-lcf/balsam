#!/bin/bash
#BSUB -nnodes {{ num_nodes }}
#BSUB -W {{ time_minutes }}
#BSUB -P {{ project }}


{{ balsam_bin }}/balsam launcher --{{ wf_filter }} --job-mode={{ job_mode }} --time-limit-minutes={{ time_minutes-2 }}
