#!/Users/misha/anaconda3/envs/testmpi/bin/python
import balsam.launcher.dag as dag
import glob

print("hello from square_pre")
print(f"jobid: {dag.current_job.pk}")
if not dag.current_job.application_args:
    print("no input file set for this job. searching workdir...")
    infile = glob.glob("side*.dat*")[0]
    dag.current_job.application_args += infile
    dag.current_job.save()
    print("set square.py input to", infile)
