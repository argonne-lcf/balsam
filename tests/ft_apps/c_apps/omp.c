#include <stdio.h>
#include <omp.h>
#include <mpi.h>

int main()
{
    MPI_Init(NULL, NULL);
    int thread = 0, nthread = 1, rank = 0, nrank = 1;

    MPI_Comm_size(MPI_COMM_WORLD, &nrank);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    char proc_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;
    MPI_Get_processor_name(proc_name, &name_len);

    #pragma omp parallel default(shared) private(thread, nthread)
    {
        #if defined (_OPENMP)
            nthread = omp_get_num_threads();
            thread = omp_get_thread_num();
        #endif
        printf("%s %d %d\n", proc_name, rank, thread);
    }
    MPI_Finalize();
    return 0;
}
