#!/Users/misha/anaconda3/envs/testmpi/bin/python
import balsamlauncher.dag as dag
import glob
import sys
import os
current_job = dag.current_job

print("Hello from make_sides_post")

if dag.ERROR:
    print("make_sides_post recognized error flag")
    num_sides = int(os.environ['BALSAM_FT_NUM_SIDES'])
    num_files = len(glob.glob("side*.dat"))
    assert num_files == num_sides
    print("it's okay, the job was actually done")
    current_job.update_state("JOB_FINISHED", "handled error; it was okay")
    exit(0)
elif dag.TIMEOUT:
    print("make_sides_post recognized timeout flag")
    num_files = len(glob.glob("side*.dat"))
    assert num_files == 0
    print("Creating rescue job")
    dag.spawn_child(clone=True, 
                            application_args="--sleep 0 --retcode 0")
    current_job.update_state("JOB_FINISHED", "spawned rescue job")
    exit(0)
elif '--dynamic-spawn' not in sys.argv:
    sys.exit(0)

reduce_job = current_job.get_child_by_name('sum_squares')

for i, sidefile in enumerate(glob.glob("side*.dat")):
    square_job = dag.spawn_child(name=f"square{i}", application="square",
                    application_args=sidefile, input_files=sidefile)
    dag.add_dependency(parent=square_job, child=reduce_job)
    print(f"spawned square{i} job")
