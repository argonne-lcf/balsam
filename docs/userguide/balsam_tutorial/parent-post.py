'''Post-processing: dynamically create jobs from output files'''
import glob
import balsam.launcher.dag as dag

reduce_job = dag.spawn_child(name = "reduce", application="reduce")

out_files = glob.glob("output*.npy")

for i, fname in enumerate(out_files):
    eig_job = dag.spawn_child(name = f"eigen{i}", application = "eigen",
                              input_files = fname, application_args = fname)

    dag.add_dependency(parent=eig_job, child=reduce_job)
