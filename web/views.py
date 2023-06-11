# views.py
#
# Copyright (C) 2011-2022 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime
from dateutil import tz
import io
import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template, 
  request, session, url_for)

from app import app, db
from decorators import authenticated, is_premium

def utc2local(utc):
    epoch = time.mktime(utc.timetuple())
    offset = datetime.fromtimestamp(epoch) - datetime.utcfromtimestamp(epoch)
    return utc + offset

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Open a connection to the S3 service
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'], 
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + "/job"

  # Define policy conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f'Unable to generate presigned URL for upload: {e}')
    return abort(500)

  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html',
    s3_post=presigned_post,
    role=session['role'])


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  region = app.config['AWS_REGION_NAME']

  # Parse redirect URL query parameters for S3 object info
  bucket_name = request.args.get('bucket')
  s3_key = request.args.get('key')

  # Extract the job ID from the S3 key
  # Move your code here
  if bucket_name is None or s3_key is None:
    return abort(404)

  key_parts = s3_key.split("~")
  key_prefix_parts = key_parts[0].split("/")
  # admin = key_prefix_parts[0]
  user_id = key_prefix_parts[1]
  job_id = key_prefix_parts[2]
  filename = key_parts[1]

  # Persist job to database
  # Move your code here...
  data = {
    "job_id" : job_id,
    "user_id" : user_id,
    "input_file_name" : filename,
    "s3_inputs_bucket" : bucket_name,
    "s3_key_input_file" : s3_key,
    "submit_time" : int(time.time()),
    "job_status" : "PENDING"
  }

  try:
    dynamodb = boto3.resource('dynamodb', region_name=region)
    ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    response = ann_table.put_item(Item = data)
  except ClientError as e:
    app.logger.error(f"Failed to upload job data to DynamoDB: {e}")
    return abort(500)

  # Send message to request queue
  # Move your code here...
  try:
    data_json = json.dumps(data)
    message = json.dumps({'default' : data_json})
    sns_client = boto3.client('sns', region_name=region)
    sns_client.publish(TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
                MessageStructure=app.config['AWS_SNS_MESSAGE_STRUCTURE'],
                Message=message)
  except ClientError as e:
    app.logger.error(f'Failed to update SNS about incoming job: {e}')
    raise abort(500)

  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  # Get list of annotations to display
  # how to query dynamodb using boto3:
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
  user_id = session['primary_identity']
  table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
  region = app.config['AWS_REGION_NAME']
  user_index = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE_USER_INDEX']
  try:
    dynamodb = boto3.client('dynamodb', region_name=region)
    response = dynamodb.query(
      ExpressionAttributeValues={
        #':v1' : {'S' : 'a571946b-7ac6-4c5b-a3a1-6ba27922bda3'},
        ':user_id' : {'S' : user_id}
      },
      KeyConditionExpression='user_id = :user_id',
      TableName=table_name,
      IndexName=user_index
    )
  except ClientError as e:
    app.logger.error(f"Failed to query jobs from DynamoDB : {e}")
    return abort(500)

  # prepare annotations data for rendering
  annotations = []
  for item in response['Items']:
    # converting utc to cst:
    # https://www.folkstalk.com/2022/10/utc-to-local-time-python-with-code-examples.html
    request_time_utc = datetime.fromtimestamp(int(item['submit_time']['N']) + 21600)
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    request_time = request_time_utc.replace(tzinfo=from_zone)
    request_time = request_time.astimezone(to_zone)
    request_time_formated = request_time.strftime("%Y-%m-%d %H:%M")
    annotation = {"job_id" : item['job_id']['S'], 
                  "request_time" : request_time_formated, 
                  "file_name" : item['input_file_name']['S'], 
                  "status" : item['job_status']['S']}
    annotations.append(annotation)

  return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  user_id = session['primary_identity']
  table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
  region = app.config['AWS_REGION_NAME']
  #user_index = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE_USER_INDEX']
  try:
    dynamodb = boto3.client('dynamodb', region_name=region)
    response = dynamodb.query(
      ExpressionAttributeValues={
        ':job_id' : {'S' : id}
      },
      KeyConditionExpression='job_id = :job_id',
      TableName=table_name
    )
  except ClientError as e:
    app.logger.error(f"Failed to query jobs from DynamoDB : {e}")
    return abort(500)
  
  # no matching job for the current authenticated user
  item = response['Items'][0]
  if item['user_id']['S'] != user_id:
    abort(403)

  from_zone = tz.tzutc()
  to_zone = tz.tzlocal()
  # handle case when the job is not done
  if not 'complete_time' in item:
    complete_time_formated = 'Still Running (Result File and Log File not ready)'
  else:
    complete_time_utc = datetime.fromtimestamp(int(item['complete_time']['N']) + 21600)
    complete_time = complete_time_utc.replace(tzinfo=from_zone)
    complete_time = complete_time.astimezone(to_zone)
    complete_time_formated = complete_time.strftime("%Y-%m-%d %H:%M")

  request_time_utc = datetime.fromtimestamp(int(item['submit_time']['N']) + 21600)
  request_time = request_time_utc.replace(tzinfo=from_zone)
  request_time = request_time.astimezone(to_zone)
  request_time_formated = request_time.strftime("%Y-%m-%d %H:%M")
  filename = item['input_file_name']['S']
  annotation = {"job_id" : item['job_id']['S'], 
                "request_time" : request_time_formated, 
                "complete_time" : complete_time_formated,
                "file_name" : filename, 
                "status" : item['job_status']['S']}

  # generate presigned url for downloading result file
  # https://stackoverflow.com/questions/60163289/how-do-i-create-a-presigned-url-to-download-a-file-from-an-s3-bucket-using-boto3
  if not 'complete_time' in item:
    presigned_url = ""
  else:
    try:
      s3 = boto3.client('s3', region_name=region, 
                        config=Config(signature_version='s3v4'))
      bucket_name = app.config['AWS_S3_RESULTS_BUCKET']
      key_name = item['s3_key_result_file']['S']
      presigned_url = s3.generate_presigned_url(
          'get_object',
          Params={'Bucket' : bucket_name, 'Key' : key_name},
          ExpiresIn=app.config['AWS_DOWNLOAD_REQUEST_EXPIRATION'])
    except ClientError as e:
      app.logger.error(f"Unable to generate presigned URL for download: {e}")
      return abort(500)

  return render_template('annotation.html', annotation=annotation, s3_download=presigned_url)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  # query db to get job detail, check if user has permission
  user_id = session['primary_identity']
  table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
  region = app.config['AWS_REGION_NAME']
  try:
    dynamodb = boto3.client('dynamodb', region_name=region)
    response = dynamodb.query(
      ExpressionAttributeValues={
        ':job_id' : {'S' : id}
      },
      KeyConditionExpression='job_id = :job_id',
      TableName=table_name
    )
  except ClientError as e:
    app.logger.error(f"Failed to query jobs from DynamoDB : {e}")
    return abort(500)
  # no matching job for the current authenticated user
  item = response['Items'][0]
  if item['user_id']['S'] != user_id:
    abort(403)

  # download log_file from s3 result bucket and serialize into string
  # https://stackoverflow.com/questions/31976273/open-s3-object-as-a-string-with-boto3
  if not 'complete_time' in item:
    log_content = "Job still running, please wait and check again"
  else:
    try:
      bucket_name = app.config['AWS_S3_RESULTS_BUCKET']
      key_name = item['s3_key_log_file']['S']
      s3 = boto3.client('s3', region_name=region)
      bytes_buffer = io.BytesIO()
      log_file = s3.download_fileobj(
          Bucket=bucket_name,
          Key=key_name,
          Fileobj=bytes_buffer)
      byte_value = bytes_buffer.getvalue()
      log_content = byte_value.decode()
    except ClientError as e:
      app.logger.error(f"Failed to download log file from s3 : {e}")
      return abort(500)

  return render_template('view_log.html', job_id=id, log_content=log_content)


"""Subscription management handler
"""
import stripe
from auth import update_profile

@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    pass
    
  elif (request.method == 'POST'):
    # Process the subscription request

    # Create a customer on Stripe

    # Subscribe customer to pricing plan

    # Update user role in accounts database

    # Update role in the session

    # Request restoration of the user's data from Glacier
    # ...add code here to initiate restoration of archived user data
    # ...and make sure you handle files not yet archived!

    # Display confirmation page
    pass


"""Set premium_user role
"""
@app.route('/make-me-premium', methods=['GET'])
@authenticated
def make_me_premium():
  # Hacky way to set the user's role to a premium user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="premium_user"
  )
  return redirect(url_for('profile'))


"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF