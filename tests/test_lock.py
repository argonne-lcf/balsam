from balsam.launcher.dag import BalsamJob
from mpi4py import MPI
import os
import socket

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
print("Rank", rank, "PID", os.getpid())

comm.barrier()

if rank == 0:
    print("")
    BalsamJob.objects.all().delete()
    jobs = [BalsamJob(name=f'job{i}') for i in range(30)]
    BalsamJob.objects.bulk_create(jobs)

comm.barrier()

to_get = BalsamJob.jobsource.all()[:7]
acquired = BalsamJob.jobsource.acquire(to_get)
acquired_names = [j.name for j in acquired]
print("Rank", rank, acquired_names)

comm.barrier()
print("Rank", rank, "has", BalsamJob.jobsource.all().count())
comm.barrier()

if rank == 0:
    print("Assertion tests...", end='')

jobs = BalsamJob.objects.all()
count = 0
for j in jobs:
    if j.name in acquired_names:
        assert j.lock.startswith(f"{socket.gethostname()}:{os.getpid()}")
        count += 1
    else:
        assert not j.lock.startswith(f"{socket.gethostname()}:{os.getpid()}")
assert count == len(acquired_names)

for name in acquired_names:
    job = BalsamJob.objects.get(name=name)
    assert job.lock.startswith(f"{socket.gethostname()}:{os.getpid()}")

for job in BalsamJob.jobsource.all():
    assert job.lock.startswith(f"{socket.gethostname()}:{os.getpid()}") or job.lock == ''

comm.barrier()
if rank == 0:
    print("Passed!\n")

if rank == 0:
    print("")
    jobs = BalsamJob.objects.order_by('lock')
    print(*((j.name, j.lock) for j in jobs), sep='\n')
