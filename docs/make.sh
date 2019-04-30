#!/bin/bash

source ~/env/bin/activate
cd ~/workflow/argobalsam/docs &&
if [ -d "_build" ]
then
    rm -r _build
fi
make html &&
open _build/html/index.html