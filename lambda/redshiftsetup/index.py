from aws_lambda_powertools import Logger
import os, boto3

logger = Logger(service="redhiftseeder", level="DEBUG")
rs = boto3.client("redshift-data")
client = boto3.client('s3')


REDSHIFT_WORKGROUP = os.environ["REDSHIFT_WORKGROUP"]
REDSHIFT_DATABASENAME = os.environ["REDSHIFT_DATABASENAME"]

def run_query_from_file(event, path):
    logger.info(path)
    with open(path, "r") as f:
        sql = f.read()
        logger.debug(sql)
        response = rs.execute_statement(Database=REDSHIFT_DATABASENAME, Sql=sql, WorkgroupName=REDSHIFT_WORKGROUP)
        logger.debug(response)

@logger.inject_lambda_context
def handler(event, context):
    logger.debug(event)
    logger.info(type(event))
    logger.info('event::', event)
    
    run_query_from_file(event, "loadscripts/load_setup.sql")
    logger.debug("requested loading!!")
    
    return event