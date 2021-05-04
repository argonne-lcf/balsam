module add atp
source /opt/intel/compilers_and_libraries_2018.0.128/linux/mkl/bin/mklvars.sh intel64 lp64

export MPICH_GNI_MAX_EAGER_MSG_SIZE=16384 
export MPICH_GNI_MAX_VSHORT_MSG_SIZE=10000 
export MPICH_GNI_MAX_EAGER_MSG_SIZE=131072 
export MPICH_GNI_NUM_BUFS=300 
export MPICH_GNI_NDREG_MAXSIZE=16777216 
export MPICH_GNI_MBOX_PLACEMENT=nic 
export MPICH_GNI_LMT_PATH=disabled 
export COMEX_MAX_NB_OUTSTANDING=6
