from collections import namedtuple
import os
import sys
import logging
import django
import signal

os.environ['DJANGO_SETTINGS_MODULE'] = 'argobalsam.settings'
django.setup()
logger = logging.getLogger('balsamlauncher.mpi_ensemble')

from subprocess import Popen, STDOUT

from mpi4py import MPI

from balsamlauncher.util import cd, get_tail
from balsamlauncher.exceptions import *

COMM = MPI.COMM_WORLD
RANK = COMM.Get_rank()

def on_exit():
    logger.debug("mpi_ensemble received interrupt: quitting now")
    MPI.Finalize()
    sys.exit(0)

handler = lambda a,b: on_exit()
signal.signal(signal.SIGINT, handler)
signal.signal(signal.SIGTERM, handler)
signal.signal(signal.SIGHUP, handler)

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


def run(job):
    basename = os.path.basename(job.workdir)
    outname = f"{basename}.out"
    logger.debug(f"mpi_ensemble rank {RANK}: starting job {job.id}")
    with cd(job.workdir) as _, open(outname, 'wb') as outf:
        try:
            status_msg(job.id, "RUNNING", msg="executing from mpi_ensemble")
            proc = Popen(job.cmd, stdout=outf, stderr=STDOUT, cwd=job.workdir)
            retcode = proc.wait()
        except Exception as e:
            logger.exception(f"mpi_ensemble rank {RANK} job {job.id}: exception during Popen")
            status_msg(job.id, "FAILED", msg=str(e))
            raise MPIEnsembleError from e
        else:
            if retcode == 0: 
                logger.debug(f"mpi_ensemble rank {RANK}: job returned 0")
                status_msg(job.id, "RUN_DONE")
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
