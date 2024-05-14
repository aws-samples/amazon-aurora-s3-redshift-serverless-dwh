from aws_lambda_powertools import Logger
import json
import boto3
import datetime
import pytz
import os
import time


client = boto3.client('s3')
jst = pytz.timezone('Asia/Tokyo')
DATASOURCE_BUCKET_NAME = os.environ['DATASOURCE_BUCKET_NAME']
logger = Logger(service="checks3object", level="DEBUG")

@logger.inject_lambda_context
def handler(event, context):
    DATE = (datetime.datetime.now(jst) - datetime.timedelta(days=1)).strftime('%Y%m%d')
    logger.debug(event)
    if event['event']['Execution']['Input']['Mode'] == 'init':
        prefix = f'init/{event["bucket"]}/'
    else:
        prefix = f'{DATE}/{event["bucket"]}/'
    try:
        paginator = client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=DATASOURCE_BUCKET_NAME, Prefix=prefix)
        result = [content['Key'] for page in pages for content in page['Contents']]
    except KeyError:
        pass

    return event