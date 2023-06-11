#!/bin/bash

# run_thaw_script.sh
#
# Copyright (C) 2011-2021 Vas Vasiliadis
# University of Chicago
#
# Runs the Glacier thawing utility script
#
##

cd /home/ubuntu/gas/util/thaw
source /usr/local/bin/virtualenvwrapper.sh
source /home/ubuntu/.virtualenvs/mpcs/bin/activate
python /home/ubuntu/gas/util/thaw/thaw_script.py

### EOF