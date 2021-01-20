#!/bin/bash
#COBALT -n {{ num_nodes }}
#COBALT -t {{ wall_time_min }}
#COBALT -A {{ project }}
#COBALT -q {{ queue }}


{{ balsam_bin }}/balsam launcher --{{ wf_filter }} --job-mode={{ job_mode }} --time-limit-minutes={{ wall_time_min-2 }}
