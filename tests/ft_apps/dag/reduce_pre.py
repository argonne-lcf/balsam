#!/Users/misha/anaconda3/envs/testmpi/bin/python
import glob
import balsam.launcher.dag as dag

square_files = glob.glob("square*.dat*")
job = dag.current_job

job.application_args=" ".join(square_files)
print(f"Have {len(square_files)} squares to sum")
job.save(update_fields=['application_args'])
