#!/bin/bash

dropdb -U postgres balsam
createdb -U postgres balsam
rm models/migrations/????_*.py

set -e
./manage.py makemigrations
./manage.py migrate
python dev/bootstrap.py