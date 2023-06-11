#!/bin/bash

# run_notify.sh
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Runs the notifier utility script
#
##

cd /home/ubuntu/gas/util/notify
source /usr/local/bin/virtualenvwrapper.sh
source /home/ubuntu/.virtualenvs/mpcs/bin/activate
python /home/ubuntu/gas/util/notify/notify.py

### EOF