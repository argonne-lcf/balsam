#!/bin/bash
#COBALT -n {{ num_nodes }}
#COBALT -t {{ time_minutes }}
#COBALT -A {{ project }}
#COBALT -q {{ queue }}


{{ balsam_bin }}/balsam launcher --{{ wf_filter }} --job-mode={{ job_mode }} --time-limit-minutes={{ time_minutes-2 }}