# notify.py
#
# Notify users of job completion
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
import psycopg2
from datetime import datetime
from dateutil import tz
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('notify_config.ini')

REGION = config['aws']['AwsRegionName']
SQS_QUEUE_NAME = config['sqs']['SqsQueueName']
SQS_WAIT_TIME = int(config['sqs']['SqsWaitTime'])
SQS_MAX_MESSAGES = int(config['sqs']['SqsMaxMessages'])
WEB_ANNOTATION_END_POINT = config['gas']['WebAnnotationEndPoint']

# initialize sqs client
try:
  sqs_client = boto3.client('sqs', region_name=REGION)
  queue_url = sqs_client.get_queue_url(QueueName=SQS_QUEUE_NAME)['QueueUrl']
except ClientError as e:
  print("Failed to connect to message queue, please restart the application")
  print(e)
  sys.exit(1)


'''Capstone - Exercise 3(d)
Reads result messages from SQS and sends notification emails.
'''
def handle_results_queue(sqs=None):
  # Read a message from the queue
  try:
    response = sqs.receive_message(
      QueueUrl=queue_url,
      MaxNumberOfMessages=SQS_MAX_MESSAGES,
      WaitTimeSeconds=SQS_WAIT_TIME
    )
  except ClientError as e:
    print("Failed to retrieve message from sqs")
    print(e)
    return
  messages = response.get('Messages', [])
  if len(messages) == 0:
    return # if no email task available skip this cycle
  
  # Process message
  message = messages[0]
  message_body = message['Body']
  receipt_handle = message['ReceiptHandle']
  try:
    message_body_obj = json.loads(message_body)
    notification_obj = json.loads(message_body_obj['Message'])
    print(notification_obj)
  except Exception as e:
    print("Failed to parse message")
    return

  # parse message, get job_id and complete_time
  job_id = notification_obj['job_id']
  user_id = notification_obj['user_id']
  complete_time = notification_obj['complete_time']
  # format compelte_time
  from_zone = tz.tzutc()
  to_zone = tz.tzlocal()
  complete_time_utc = datetime.fromtimestamp(complete_time + 21600)
  complete_time = complete_time_utc.replace(tzinfo=from_zone)
  complete_time = complete_time.astimezone(to_zone)
  complete_time_formated = complete_time.strftime("%Y-%m-%d %H:%M")
  # prepare html body and subject
  email_subject = f"Results available for job {job_id}"
  email_body = f"<p>Your annotation job completed at {complete_time_formated} . \
                Click here to view job details and results: \
                <a href={WEB_ANNOTATION_END_POINT}/{job_id}>view</a></p>"
  # get user email
  recipient_email = helpers.get_user_profile(user_id)['email']
  # call helper to send email
  helpers.send_email_ses(recipients=recipient_email, subject=email_subject, body=email_body)
  print(f"Notification email sent to {recipient_email}")
  # Delete message
  try:
    response = sqs_client.delete_message(QueueUrl=queue_url,ReceiptHandle=receipt_handle)
  except ClientError as e:
    print("Failed to delete message from sqs")
    print(e)
  pass


if __name__ == '__main__':
  
  # Get handles to resources; and create resources if they don't exist

  # Poll queue for new results and process them
  while True:
    handle_results_queue(sqs=sqs_client)

### EOF
