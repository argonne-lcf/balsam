# HPC Edge Service and Workflow Management System
**Authors:** J. Taylor Childers (Argonne National Laboratory), Tom Uram (Argonne National Laboratory), Doug Benjamin (Duke University), Misha Salim (Argonne National Laboratory)

An HPC Edge Service to manage remote job submission. The goal of this service is to provide a secure interface for submitting jobs to large computing resources.

# Prerequisites
The Argo and Balsam services require Python 3.6, mpi4py, Django, and django-concurrency.

To establish the needed environment on Cooley or Theta, it is recommended to use Anaconda:
### Cooley:
```
soft add +anaconda
conda config --add channels intel
conda create --name balsam intelpython3_full python=3
source ~/.conda/envs/balsam/bin/activate balsam
pip install django django-concurrency
```
### Additional steps on Theta:
    cp -i  /opt/cray/pe/mpt/7.6.0/gni/mpich-intel-abi/16.0/lib/libmpi* ~/.conda/envs/balsam/lib/
    # “yes” to overwrite libmpi.so.12 and libmpifort.so.12
    export LD_LIBRARY_PATH=~/.conda/envs/balsam/lib:$LD_LIBRARY_PATH



# Installation
```
git clone git@xgitlab.cels.anl.gov:turam/hpc-edge-service.git
cd hpc-edge-service
source activate balsam
virtualenv argobalsam_env
source argobalsam_env/bin/activate
pip install pip --upgrade
pip install django
pip install pika
pip install future
export ARGOBALSAM_INSTALL_PATH=$PWD
mkdir log argojobs balsamjobs exe
```

# Configure Databases
You can find many settings to change. There are Django specific settings in `argobalsam/settings.py` and Edge Service settings in `user_settings.py`.

To create and initialize the default sqlite3 database without password protections do:
```
./manage.py makemigrations argo
./manage.py makemigrations balsam
./manage.py migrate
./manage -h
```



