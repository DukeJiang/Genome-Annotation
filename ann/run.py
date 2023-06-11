# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import driver
import boto3
import logging
from botocore.exceptions import ClientError
import os
import json
from configparser import ConfigParser

config = ConfigParser(os.environ)
config.read('ann_config.ini')

INPUT_BUCKET_NAME = config['s3']['InputBucketName']
RESULT_BUCKET_NAME = config['s3']['ResultBucketName']
REGION = config['aws']['AwsRegionName']
ANNOTATOR_BASE_DIR = config['ann']['AnnotatorBaseDir']
ANNOTATOR_JOBS_DIR = config['ann']['AnnotatorJobsDir']
DYNAMO_DB_TABLE = config['dynamodb']['DynamoDBTable']
SNS_JOB_RESULT_TOPIC = config['sns']['SnsJobResultTopic']
SNS_MESSAGE_STRUCTURE = config['sns']['SnsMessageStructure']

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")


# how to upload file with boto3:
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
def upload_file(file_name, bucket, object_name=None):
  if object_name is None:
    object_name = os.path.basename(file_name)
  s3_client = boto3.client('s3')
  try:
    response = s3_client.upload_file(file_name, bucket, object_name)
  except ClientError as e:
    logging.error(e)
    return False
  return True


if __name__ == '__main__':
  # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')
        # Add code here:
        # 1. Upload the results file to S3 results bucket
        # # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
        s3 = boto3.client('s3')

        # eg : '/home/ubuntu/gas/ann/jobs/yuxuanjiang/userX/8eee552a-af9d-4538-b8d2-6da9cf82fccb/test.vcf'
        filepath = sys.argv[1]
        filepath_parts = filepath.split("/")
        input_filename = filepath[9]
        input_file_parts = filepath_parts[9].split(".")
        result_filename = f"{input_file_parts[0]}.annot.vcf"
        result_object_key = f"{filepath_parts[6]}/{filepath_parts[7]}/{filepath_parts[8]}/{result_filename}"
        result_filepath = f"{ANNOTATOR_JOBS_DIR}{result_object_key}"
        print(f"Writing from {result_filepath}")

        # upload result file to s3 results bucket
        if upload_file(result_filepath, RESULT_BUCKET_NAME, result_object_key):
          print("Result uploaded to bucket")
        else:
          print("Result upload failed")

        # 2. Upload the log file to S3 results bucket
        log_filename = f"{input_file_parts[0]}.vcf.count.log"
        log_object_key = f"{filepath_parts[6]}/{filepath_parts[7]}/{filepath_parts[8]}/{log_filename}"
        log_filepath = f"{ANNOTATOR_JOBS_DIR}{log_object_key}"
        if upload_file(log_filepath, RESULT_BUCKET_NAME, log_object_key):
          print("Log file uploaded to bucket")
        else:
          print("Log file upload fialed")

        # 3. Clean up (delete) local job files
        # https://pynative.com/python-delete-files-and-directories/
        admin = filepath_parts[6]
        user = filepath_parts[7]
        job_id = filepath_parts[8]
        # remove all files in jobs folder 
        for file_2_delete in os.listdir(f"{ANNOTATOR_JOBS_DIR}{admin}/{user}/{job_id}"):
          file = ANNOTATOR_JOBS_DIR + file_2_delete
          if os.path.isfile(file):
            print('Deleting file:', file)
            os.remove(file)

        # update db job status to COMPLETE and insert additional fields
        # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
        try:
          dynamodb = boto3.resource('dynamodb', region_name=REGION)
          ann_table = dynamodb.Table(DYNAMO_DB_TABLE)
          response = ann_table.update_item(
                        Key = {"job_id" : job_id},
                        ExpressionAttributeValues = {
                                                      ":s" : "COMPLETED",
                                                      ":t" : int(time.time()),
                                                      ":krf" : result_object_key,
                                                      ":klf" : log_object_key
                                                    },
                        UpdateExpression = """SET job_status = :s,
                                              complete_time = :t,
                                              s3_key_result_file = :krf,
                                              s3_key_log_file = :klf
                                            """,
                        ReturnValues="UPDATED_OLD"
                      )
        except ClientError as e:
          print("Update job status failed")
          raise e
        
        # job in a settled state, prepare notification message to user
        data = {
          "job_id" : job_id,
          "user_id" : user,
          "input_file_name" : input_filename,
          "s3_inputs_butket" : INPUT_BUCKET_NAME,
          "complete_time" : int(time.time())
        }
        try:
          data_json = json.dumps(data)
          message = json.dumps({'default' : data_json})
          sns_client = boto3.client('sns', region_name=REGION)
          sns_client.publish(TopicArn=SNS_JOB_RESULT_TOPIC,
                            MessageStructure=SNS_MESSAGE_STRUCTURE,
                            Message=message)
        except ClientError as e:
          logging.info(f"Failed to publish notification message to user: {e}")
          raise e

    else:
        print("A valid .vcf file must be provided as input to this program.")

### EOF