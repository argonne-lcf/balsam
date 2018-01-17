import balsam.launcher.dag as dag

slow_job = dag.BalsamJob.objects.get(name='slow_job')
dag.kill(slow_job)
