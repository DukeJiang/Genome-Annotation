#!/bin/bash

# run_archive_script.sh
#
# Copyright (C) 2011-2021 Vas Vasiliadis
# University of Chicago
#
# Runs the archive utility script
#
##

cd /home/ubuntu/gas/util/archive
source /usr/local/bin/virtualenvwrapper.sh
source /home/ubuntu/.virtualenvs/mpcs/bin/activate
python /home/ubuntu/gas/util/archive/archive_script.py

### EOF