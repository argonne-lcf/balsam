#!/bin/bash

export BALSAM_LOG_DIR="/balsam/log"
mkdir -p $BALSAM_LOG_DIR
balsam server migrate
exec balsam server exec-gunicorn