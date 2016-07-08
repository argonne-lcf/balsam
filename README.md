# HPC Edge Service

An HPC Edge Service to manage remote job submission. The goal of this service is to provide a secure interface for submitting jobs to large computing resources.


# Installation
```
git clone git@github.com:hep-cce/hpc-edge-service.git
cd hpc-edge-service
virtualenv argobalsam_env
source argobalsam_env/bin/activate
argobalsam_env/bin/pip install django
argobalsam_env/bin/pip install pika
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



