# thaw_script.py
#
# Thaws upgraded (premium) user data
#
# Copyright (C) 2011-2021 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import boto3
import time
import os
import sys
import json
from botocore.exceptions import ClientError

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('thaw_script_config.ini')

'''Capstone - Exercise 9
Initiate thawing of archived objects from Glacier
'''
def handle_thaw_queue(sqs=None):
  
  # Read a message from the queue

  # Process message

  # Delete message

  pass

if __name__ == '__main__':  

  # Get handles to resources; and create resources if they don't exist

  # Poll queue for new results and process them
  while True:
    handle_thaw_queue(sqs=sqs)

### EOF