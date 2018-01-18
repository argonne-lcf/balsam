'''mpi4py wrapper that allows an ensemble of serial applications to run in
parallel across ranks on the computing resource'''
from collections import namedtuple
import os
import sys
import logging
import django
import signal

os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
django.setup()
logger = logging.getLogger('balsam.launcher.mpi_ensemble')

from subprocess import Popen, STDOUT, TimeoutExpired

from mpi4py import MPI

from balsam.launcher.util import cd, get_tail
from balsam.launcher.exceptions import *
from balsam.service.models import BalsamJob

COMM = MPI.COMM_WORLD
RANK = COMM.Get_rank()
HANDLE_EXIT = False

def on_exit(job):
    global HANDLE_EXIT
    if HANDLE_EXIT: return
    HANDLE_EXIT = True

    logger.debug(f"mpi_ensemble.py rank {RANK} received interrupt: quitting now")
    if job is not None:
        job.terminate()
        try: job.wait(timeout=10)
        except: job.kill()
    print("TIMEOUT")
    MPI.Finalize()
    sys.exit(0)

handler = lambda a,b: on_exit(None)
signal.signal(signal.SIGINT, handler)
signal.signal(signal.SIGTERM, handler)

Job = namedtuple('Job', ['id', 'workdir', 'cmd'])


def status_msg(id, state, msg=''):
    print(f"{id} {state} {msg}", flush=True)


def read_jobs(fp):
    for line in fp:
        try:
            id, workdir, *command = line.split()
            logger.debug(f"Read Job {id}  CMD: {command}  DIR: {workdir}")
        except:
            logger.debug("Invalid jobline")
            continue
        if id and command and os.path.isdir(workdir):
            yield Job(id, workdir, command)
        else:
            logger.debug("Invalid workdir")

def poll_execution_or_killed(job, proc, period=10):
    retcode = None
    while retcode is None:
        try:
            retcode = proc.wait(timeout=period)
        except TimeoutExpired:
            job.refresh_from_db()
            if job.state == 'USER_KILLED':
                logger.debug(f"{job.cute_id} USER_KILLED; terminating it now")
                proc.terminate()
                return "USER_KILLED"
        else:
            return retcode

def run(job):
    job_from_db = BalsamJob.objects.get(pk=job.id)

    if job_from_db.state == 'USER_KILLED':
        status_msg(job.id, "USER_KILLED", msg="mpi_ensemble skipping this job")
        return

    basename = job_from_db.name
    outname = f"{basename}.out"
    logger.debug(f"mpi_ensemble rank {RANK}: starting job {job.id}")
    with cd(job.workdir) as _, open(outname, 'wb') as outf:
        try:
            status_msg(job.id, "RUNNING", msg="executing from mpi_ensemble")

            env = job_from_db.get_envs() # TODO: Should we include this?
            proc = Popen(job.cmd, stdout=outf, stderr=STDOUT,
                         cwd=job.workdir,env=env)

            handler = lambda a,b: on_exit(proc)
            signal.signal(signal.SIGINT, handler)
            signal.signal(signal.SIGTERM, handler)

            retcode = poll_execution_or_killed(job_from_db, proc)

        except Exception as e:
            logger.exception(f"mpi_ensemble rank {RANK} job {job.id}: exception during Popen")
            status_msg(job.id, "FAILED", msg=str(e))
            raise MPIEnsembleError from e
        else:
            if retcode == 0: 
                logger.debug(f"mpi_ensemble rank {RANK}: job returned 0")
                status_msg(job.id, "RUN_DONE")
            elif retcode == "USER_KILLED":
                status_msg(job.id, "USER_KILLED", msg="mpi_ensemble aborting job due to user request")
            else:
                outf.flush()
                tail = get_tail(outf.name).replace('\n', '\\n')
                msg = f"NONZERO RETURN {retcode}: {tail}"
                status_msg(job.id, "RUN_ERROR", msg=msg)
                logger.debug(f"mpi_ensemble rank {RANK} job {job.id} {msg}")
        finally:
            proc.kill()


def main(jobs_path):
    job_list = None

    if RANK == 0:
        logger.debug(f"Master rank of mpi_ensemble.py: reading jobs from {jobs_path}")
        with open(jobs_path) as fp: 
            job_list = list(read_jobs(fp))

    job_list = COMM.bcast(job_list, root=0)
    if RANK == 0:
        logger.debug(f"Broadcasted job list. Total {len(job_list)} jobs to run")
    for job in job_list[RANK::COMM.size]: run(job)

if __name__ == "__main__":
    path = sys.argv[1]
    main(path)
