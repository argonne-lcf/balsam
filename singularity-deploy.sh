#!/bin/bash

POSTGRES_USER="postgres"
POSTGRES_PASSWORD="postgres"
POSTGRES_DB="balsam"

export POSTGRES_USER
export POSTGRES_PASSWORD
export POSTGRES_DB

BALSAM_LOG_DIR="./balsam-logs"
BALSAM_DATABASE_URL="postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:5432/$POSTGRES_DB"
SERVER_PORT="800"
BALSAM_REDIS_PARAMS='{"host": "localhost", "port": "6379"}'
BALSAM_LOG_LEVEL="INFO"
BALSAM_LOG_DIR="./balsam-logs"
      

export BALSAM_LOG_DIR
export BALSAM_DATABASE_URL
export SERVER_PORT
export BALSAM_REDIS_PARAMS
export BALSAM_LOG_LEVEL
export BALSAM_LOG_DIR


mkdir -p db-control
mkdir -p db-data
mkdir -p $BALSAM_LOG_DIR

singularity run \
  -B ./db-control:/var/run/postgresql \
  -B ./db-data:/var/lib/postgresql/data \
  docker://postgres &
sleep 2

singularity run  docker://redis &
sleep 2
  
singularity run \
  -B $BALSAM_LOG_DIR:/balsam/log \
  docker://masalim2/balsam:develop &

wait
