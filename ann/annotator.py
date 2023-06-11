import boto3
import json
import subprocess
import sys
import os
from botocore.exceptions import ClientError
from pathlib import Path
from configparser import ConfigParser

config = ConfigParser(os.environ)
config.read('ann_config.ini')
# print(config['aws']['AwsRegionName'])

INPUT_BUCKET_NAME = config['s3']['InputBucketName']
SQS_QUEUE_NAME = config['sqs']['SqsQueueName']
REGION = config['aws']['AwsRegionName']
ANNOTATOR_BASE_DIR = config['ann']['AnnotatorBaseDir']
ANNOTATOR_JOBS_DIR = config['ann']['AnnotatorJobsDir']
DYNAMO_DB_TABLE = config['dynamodb']['DynamoDBTable']

s3 = boto3.resource('s3', region_name=REGION)
# Connect to SQS and get the message queue
# How to get queue url
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.get_queue_url
sqs_client = boto3.client('sqs', region_name=REGION)
try:
    queue_url = sqs_client.get_queue_url(QueueName=SQS_QUEUE_NAME)['QueueUrl']
except ClientError as e:
    print("Failed to connect to message queue, please restart the application")
    print(e)
    sys.exit(1)

# Poll the message queue in a loop 
while True:
    # Attempt to read a message from the queue
    # Use long polling - DO NOT use sleep() to wait between polls
    # How to do long polling:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
    # Information on long polling:
    # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-long-polling
    try:
        response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5,
            )
    except ClientError as e:
        print("Failed to retrieve message from sqs")
        print(e)
        continue
    messages = response.get('Messages', [])
    #print(messages)
    if len(messages) == 0: 
        continue # if no job availble, skip this cycle        

    # If message read, extract job parameters from the message body as before
    message = messages[0]
    message_body = message['Body']
    receipt_handle = message['ReceiptHandle']
    try:
        message_body_obj = json.loads(message_body)
        #print(message_body_obj)
        job_obj = json.loads(message_body_obj['Message'])
        print(job_obj)
    except Exception as e:
        print("Failed to parse message")
        continue
    # extract job parameters from job_obj
    job_id = job_obj['job_id']
    user_id = job_obj['user_id']
    input_file_name = job_obj['input_file_name']
    s3_inputs_bucket = job_obj['s3_inputs_bucket']
    s3_key_input_file = job_obj['s3_key_input_file']
    submit_time = job_obj['submit_time']
    admin_id = s3_key_input_file.split("/")[0]


    # Include below the same code you used in prior homework
    # Get the input file S3 object and copy it to a local file
    # Use a local directory structure that makes it easy to organize
    # multiple running annotation jobs
    
    # create the directory that the new job will live in
    directory = f"{ANNOTATOR_JOBS_DIR}{admin_id}/{user_id}/{job_id}/"
    Path(directory).mkdir(parents=True, exist_ok=True)
    filepath = directory + input_file_name
    # get file object from s3 and put it in the new directory, rename it with filename
    try:
        s3.Bucket(INPUT_BUCKET_NAME).download_file(s3_key_input_file, filepath)
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            print('File not found')
        continue

    # Launch annotation job as a background process
    try:
        job = subprocess.Popen(["python", f"{ANNOTATOR_BASE_DIR}run.py", filepath])
    except subprocess.CalledProcessError as e:
        print("Failed to start annotation subprocess")
        print(e)
        continue

    # update db, persist record of precess to running state
    # how to user update_item:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
    try:
        dynamodb = boto3.resource('dynamodb', region_name=REGION)
        ann_table = dynamodb.Table(DYNAMO_DB_TABLE)
        db_update_response = ann_table.update_item(
                                Key = {'job_id' : job_id},
                                ExpressionAttributeValues = {':s' : 'RUNNING', ':js' : 'PENDING'},
                                ConditionExpression = "begins_with(job_status, :js)",
                                UpdateExpression = 'SET job_status = :s',
                                ReturnValues = "UPDATED_OLD"
                            )
    except ClientError as e:
        print("Failed to update job status")

    # Delete the message from the queue, if job was successfully submitted
    # How to delete a message from sqs:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
    try:
        response = sqs_client.delete_message(
                                    QueueUrl=queue_url,
                                    ReceiptHandle=receipt_handle
                                )
    except ClientError as e:
        print("Failed to delete message from sqs")
        print(e)
