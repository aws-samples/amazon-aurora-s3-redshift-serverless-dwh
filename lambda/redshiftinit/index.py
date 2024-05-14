from aws_lambda_powertools import Logger
import os, boto3, time

logger = Logger(service="redhiftInit", level="DEBUG")
rs = boto3.client("redshift-data")

REDSHIFT_WORKGROUP = os.environ["REDSHIFT_WORKGROUP"]
REDSHIFT_NAMESPACE = os.environ["REDSHIFT_NAMESPACE"]
REDSHIFT_DATABASENAME = os.environ["REDSHIFT_DATABASENAME"]
logger.info('REDSHIFT_WORKGROUP', REDSHIFT_WORKGROUP)
logger.info('REDSHIFT_NAMESPACE', REDSHIFT_NAMESPACE)
logger.info('REDSHIFT_DATABASENAME', REDSHIFT_DATABASENAME)


def exec_statement(sql, database=REDSHIFT_DATABASENAME):
    response = rs.execute_statement(Database=database, Sql=sql, WorkgroupName=REDSHIFT_WORKGROUP)
    logger.info('Get Responce')
    id = response["Id"]
    while True:
        response = rs.describe_statement(Id=id)
        status = response["Status"]
        if status == "FAILED":
            logger.error(response)
            raise Exception()
        if status == "FINISHED":
            logger.debug(response)
            break
        time.sleep(5)
    return


def run_query_from_file(path):
    logger.info(path)
    with open(path, "r") as f:
        sql = f.read()
        exec_statement(sql)


@logger.inject_lambda_context
def handler(event, context):
    logger.info(event)

    run_query_from_file("schema/products_master.sql")
    run_query_from_file("schema/orders_master.sql")
    run_query_from_file("schema/order_details_master.sql")
