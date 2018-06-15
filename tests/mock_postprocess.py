'''Mock user postprocess script; testing out the Python API provided in
balsamlauncher.dag'''

import sys
import balsam.launcher.dag as dag

def mock_spawn():
    child = dag.spawn_child(clone=True, name='spawned_child', state='CREATED')

def mock_addjobs():
    job1 = dag.add_job(name="added1")
    job2 = dag.add_job(name="added2")
    job3 = dag.add_job(name="added3")
    dag.add_dependency(parent=job2, child=job3)

def mock_kill():
    current_job = dag.current_job
    dag.kill(current_job, recursive=True)


if __name__ == "__main__":
    this_module = sys.modules[__name__]
    keyword = sys.argv[1]
    fxn = getattr(this_module, f'mock_{keyword}')
    fxn()
