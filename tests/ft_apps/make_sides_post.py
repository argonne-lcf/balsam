#!/Users/misha/anaconda3/envs/testmpi/bin/python
import balsam.launcher.dag as dag
import glob
import sys
import os
current_job = dag.current_job

print("Hello from make_sides_post")

if dag.ERROR or dag.TIMEOUT:
    if dag.ERROR: 
        print("make_sides_post recognized error flag")
    else: 
        print("make_sides_post recognized timeout flag")

    num_sides = int(os.environ['BALSAM_FT_NUM_SIDES'])
    num_files = len(glob.glob("side*.dat"))

    if num_files == num_sides:
        print("it's okay, the job was actually done")
        current_job.update_state("JOB_FINISHED", "handled error; it was okay")
        exit(0)
    elif num_files == 0:
        print("Creating rescue job")
        children = current_job.get_children()
        rescue = dag.spawn_child(clone=True, application_args="--sleep 0 --retcode 0")
        rescue.set_parents([])
        current_job.update_state("JOB_FINISHED", f"spawned rescue job {rescue.cute_id}")
        for child in children:
            child.set_parents([rescue])
        exit(0)

if '--dynamic-spawn' not in sys.argv:
    sys.exit(0)

reduce_job = current_job.get_child_by_name('sum_squares')

for i, sidefile in enumerate(glob.glob("side*.dat")):
    square_job = dag.spawn_child(name=f"square{i}", application="square",
                    application_args=sidefile, input_files=sidefile)
    dag.add_dependency(parent=square_job, child=reduce_job)
    print(f"spawned square{i} job")
