from collections import namedtuple
import os
import sys
import logging
import django
os.environ['DJANGO_SETTINGS_MODULE'] = 'argobalsam.settings'
django.setup()
logger = logging.getLogger('balsamlauncher.mpi_ensemble')

from subprocess import Popen, STDOUT

from mpi4py import MPI
from balsamlauncher.cd import cd
from balsamlauncher.exceptions import *

COMM = MPI.COMM_WORLD
RANK = COMM.Get_rank()

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
    logger.debug(f"Running job {job.id}")
    with cd(job.workdir) as _, open(outname, 'wb') as outf:
        try:
            status_msg(job.id, "RUNNING", msg="executing from mpi_ensemble")
            proc = Popen(job.cmd, stdout=outf, stderr=STDOUT)
            retcode = proc.wait()
        except Exception as e:
            status_msg(job.id, "FAILED", msg=str(e))
            raise MPIEnsembleError from e
        else:
            if retcode == 0: status_msg(job.id, "RUN_DONE")
            else: status_msg(job.id, "RUN_ERROR", msg=f"process return code {retcode}")
        finally:
            proc.kill()


def main(jobs_path):
    job_list = None

    if RANK == 0:
        with open(jobs_path) as fp: 
            job_list = list(read_jobs(fp))

    job_list = COMM.bcast(job_list, root=0)
    logger.debug(f"Broadcasted job list. Total {len(job_list)} jobs to run")
    for job in job_list[RANK::COMM.size]: run(job)

if __name__ == "__main__":
    path = sys.argv[1]
    logger.debug(f"Starting mpi_ensemble.py. Reading jobs from {path}")
    main(path)
