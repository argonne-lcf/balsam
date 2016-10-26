#!/usr/bin/env bash

####
# you can place additional environment needs here
#########


#####
# set pathas for edge service
################################
export ARGOBALSAM_INSTALL_PATH=FIXME
export ARGOBALSAM_DATA_PATH=$ARGOBALSAM_INSTALL_PATH
export ARGOBALSAM_EXE_PATH=$ARGOBALSAM_INSTALL_PATH/exe

#####
# activate the virtualenv 
################################
. $ARGOBALSAM_INSTALL_PATH/argobalsam_env/bin/activate


#####
# setup the certificate info
#######################################
. grid_setup.sh



