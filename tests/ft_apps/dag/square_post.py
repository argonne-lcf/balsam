#!/Users/misha/anaconda3/envs/testmpi/bin/python
import balsam.launcher.dag as dag

print("hello from square_post")
print(f"jobid: {dag.current_job.pk}")
if dag.ERROR:
    print("recognized error")
    dag.current_job.update_state("JOB_FINISHED", "handled error in square_post")
if dag.TIMEOUT:
    print("recognized timeout")
    dag.current_job.update_state("JOB_FINISHED", "handled timeout in square_post")
