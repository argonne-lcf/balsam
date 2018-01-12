#include <mpi.h>
#include <omp.h>
#include <string.h>

int main (int argc, char **argv)
{
    int rank;
    int size;
    int thread;
    int threads;
    int jid;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    jid = atoi(getenv("ALPS_APP_ID"));
    threads = omp_get_num_threads();

#pragma omp parallel private(thread)
    {
        thread = omp_get_thread_num();
        printf("hello from thread: %d\n", thread);
    }

    if (rank == 0)
      printf("Job: %d Ranks: %d Threads: %d\n", jid, size, threads);

    MPI_Finalize();
    return 0;
}
