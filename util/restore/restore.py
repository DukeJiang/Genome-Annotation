# restore.py
#
# Restores thawed data, saving objects to S3 results bucket
# NOTE: This code is for an AWS Lambda function
#
# Copyright (C) 2011-2021 Vas Vasiliadis
# University of Chicago
##

import boto3
import time
import os
import sys
import json
from botocore.exceptions import ClientError

# Define constants here; no config file is used for Lambdas
DYNAMODB_TABLE = "<CNetID>_annotations"

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    pass

### EOF