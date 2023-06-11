#!/bin/bash

# run_thaw_app.sh
#
# Copyright (C) 2011-2021 Vas Vasiliadis
# University of Chicago
#
# Runs the Glacier thawing utility Flask app
#
##

SSL_CERT_PATH=/usr/local/src/ssl/ucmpcs.org.crt
SSL_KEY_PATH=/usr/local/src/ssl/ucmpcs.org.key

cd /home/ubuntu/gas

export ARCHIVE_APP_HOME=/home/ubuntu/gas/util/thaw
export SOURCE_HOST=0.0.0.0
export HOST_PORT=4433

/home/ubuntu/.virtualenvs/mpcs/bin/uwsgi \
  --manage-script-name \
  --enable-threads \
  --vacuum \
  --log-master \
  --chdir $ARCHIVE_APP_HOME \
  --socket /tmp/thaw_app.sock \
  --mount /thaw_app=thaw_app:app \
  --https $SOURCE_HOST:$HOST_PORT,$SSL_CERT_PATH,$SSL_KEY_PATH

### EOF