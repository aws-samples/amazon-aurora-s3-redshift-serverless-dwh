from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities import parameters
import json
import os
import boto3
import pymysql
import pytz
import datetime
import ast

jst = pytz.timezone('Asia/Tokyo')

DB_SECRET_NAME = os.environ['DB_SECRET_NAME']
DB_HOSTNAME = os.environ['DB_HOSTNAME']
DB_NAME = os.environ['DB_NAME']
DATASOURCE_BUCKET_NAME = os.environ['DATASOURCE_BUCKET_NAME']

secret = json.loads(parameters.get_secret(DB_SECRET_NAME, max_age=60))
logger = Logger(service='RDSloadKicker', level='DEBUG')

s3 = boto3.client('s3')

def grain_access_rds_s3(input_data):
    with pymysql.connect(host=DB_HOSTNAME, db=DB_NAME, user=secret['username'], password=secret['password'],autocommit=True) as conn:
        with conn.cursor() as cur:
            # Aurora MySQL version 3
            cur.execute(f"GRANT AWS_SELECT_S3_ACCESS TO '{input_data['user']}'@'{input_data['domain-or-ip-address']}';")
            # Aurora MySQL version 2
            # cur.execute(f"GRANT SELECT INTO S3 ON *.* TO '{input_data['user']}'@'{input_data['domain-or-ip-address']}';")



def run_query_from_file(bucket, input_data, path):
    logger.debug(path)
    try:
        conn = pymysql.connect(host=DB_HOSTNAME, db=DB_NAME, user=secret['username'], password=secret['password'],autocommit=True)
        logger.debug('connect Done')
    except Exception as e:
        logger.error('ERROR: Unexpected error: Could not connect to MySQL instance.')
        logger.error(e)
    with open(path, 'r') as f:
        sql = f.read()
        sql = sql.replace('<TABLE>', bucket)
        
        if input_data['Mode'] == 'Operation':
            sql = sql.replace('<DATE>', input_data["date"])
            
        elif input_data['Mode'] == 'init' and input_data['Term'] == 'specific':
            sql = sql.replace('<DATE>', input_data['Day'])
            
        elif input_data['Mode'] == 'init' and input_data['Term'] == 'all':
            sql = sql.replace('<DATE>', 'init')

        sql = sql.replace('<DATASOURCE_BUCKET_NAME>', DATASOURCE_BUCKET_NAME)
        logger.debug(sql)
    
    with conn.cursor() as cur:       
        cur.execute(sql)
    conn.close()

@logger.inject_lambda_context
def handler(event, context):
    logger.debug(event)
    bucket = event["bucket"]
    input_data = event['event']["Execution"]["Input"]
    input_data["date"] = (datetime.datetime.now(jst) - datetime.timedelta(days=1)).strftime('%Y%m%d')

    if input_data['Mode'] == 'Operation':
        run_query_from_file(bucket, input_data,'loadscripts/aurora_to_s3.sql')
    elif input_data['Mode'] == 'init' and input_data['Term'] == 'specific':
        grain_access_rds_s3(input_data)
        run_query_from_file(bucket, input_data,'loadscripts/aurora_to_s3.sql')
    elif input_data['Mode'] == 'init' and input_data['Term'] == 'all': 
        grain_access_rds_s3(input_data)
        run_query_from_file(bucket, input_data,'loadscripts/aurora_to_s3_init.sql')
    else:
        logger.error('Arguments other than those specified have been set. Please check the arguments.')
    
    return input_data
