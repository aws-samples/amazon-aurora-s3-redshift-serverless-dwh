from aws_lambda_powertools import Logger
import os, boto3, time
import pandas as pd
import pytz
import datetime
from io import StringIO

jst = pytz.timezone('Asia/Tokyo')

logger = Logger(service="redhiftLoadKicker", level="DEBUG")
rs = boto3.client("redshift-data")
client = boto3.client('s3')


REDSHIFT_WORKGROUP = os.environ["REDSHIFT_WORKGROUP"]
REDSHIFT_NAMESPACE = os.environ["REDSHIFT_NAMESPACE"]
REDSHIFT_DATABASENAME = os.environ["REDSHIFT_DATABASENAME"]
ROLEARN_TO_READ_DATASOURCE = os.environ["ROLEARN_TO_READ_DATASOURCE"]
DATASOURCE_BUCKET_NAME = os.environ["DATASOURCE_BUCKET_NAME"]


def run_query_from_file(input_data, path):
    logger.info(path)
    with open(path, "r") as f:
        sql = f.read()
        sql = sql.replace('<ROLEARN_TO_READ_DATASOURCE>', ROLEARN_TO_READ_DATASOURCE)
        if input_data['Mode'] == 'Operation':
            sql = sql.replace('<DATE>', input_data["date"])
        elif input_data['Mode'] =='init' and  input_data['Term'] =='specific':
            sql = sql.replace('<DATE>',  input_data['Day'])    
        else:
            sql = sql.replace('<DATE>', 'init')
        sql = sql.replace('<DATASOURCE_BUCKET_NAME>', DATASOURCE_BUCKET_NAME)
        logger.debug(sql)
        response = rs.execute_statement(Database=REDSHIFT_DATABASENAME, Sql=sql, WorkgroupName=REDSHIFT_WORKGROUP)
        logger.debug(response)
        return response["Id"]



@logger.inject_lambda_context
def handler(event, context):
    logger.info('event::', event)
    datatype = event['environments']['bucket']
    input_data =  event['environments']['event']['Execution']['Input']
    input_data["date"] = (datetime.datetime.now(jst) - datetime.timedelta(days=1)).strftime('%Y%m%d')

    if input_data['Mode'] == 'Operation':
        prefix = f'{input_data["date"]}/{datatype}/'
    elif input_data['Mode'] =='init' and input_data['Term'] =='specific':
        prefix =  f'{input_data["Day"]}/{datatype}/'
    elif input_data['Mode'] =='init' and input_data['Term'] =='all':
        prefix = f'init/{datatype}/'
    
    try:
        paginator = client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=DATASOURCE_BUCKET_NAME, Prefix=prefix)
        result = [content['Key'] for page in pages for content in page['Contents']]

    except KeyError:
        logger.debug("No directry.")
        return {'event': 'pass'}
    
    ##Check the recode in CSV
    record_data_in_csv = client.get_object(Bucket=DATASOURCE_BUCKET_NAME, Key=f'{prefix}{datatype}.csv.part_00000')
    
    if record_data_in_csv:
        request_id = run_query_from_file(input_data, f"loadscripts/load_{datatype}.sql")
        logger.debug("requested loading!!")
        logger.debug(request_id)
        return {'id': request_id}
    else:
        logger.debug("No record in CSV")
        return {'event': 'pass'}