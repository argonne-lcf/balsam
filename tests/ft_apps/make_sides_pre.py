#!/Users/misha/anaconda3/envs/testmpi/bin/python
import balsam.launcher.dag as dag
import sys

num_sides = sys.argv[1]
num_ranks = sys.argv[2]

current_job = dag.current_job
current_job.environ_vars=f"BALSAM_FT_NUM_SIDES={num_sides}:BALSAM_FT_NUM_RANKS={num_ranks}"
current_job.ranks_per_node = num_ranks
current_job.save(update_fields=['environ_vars', 'ranks_per_node'])

print("Hello from make_sides_pre")
