from aws_lambda_powertools import Logger
import os, boto3, time

logger = Logger(service="redhiftLoadWaiter", level="DEBUG")
rs = boto3.client("redshift-data")

REDSHIFT_WORKGROUP = os.environ["REDSHIFT_WORKGROUP"]
REDSHIFT_NAMESPACE = os.environ["REDSHIFT_NAMESPACE"]
REDSHIFT_DATABASENAME = os.environ["REDSHIFT_DATABASENAME"]


def is_loading_completed(load_request_id):
    response = rs.describe_statement(Id=load_request_id)
    status = response["Status"]
    if status == "FAILED":
        logger.error(response)
        raise Exception()
    if status == "FINISHED":
        logger.debug(response)
        return True

    return False

@logger.inject_lambda_context
def handler(event, context):
    logger.info(event)
    if ('id' in event):
        request_id = event['id']
        is_completed = is_loading_completed(request_id)
        if not is_completed:
            return False
    else:
        return True