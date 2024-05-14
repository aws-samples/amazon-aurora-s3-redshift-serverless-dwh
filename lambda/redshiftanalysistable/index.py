from aws_lambda_powertools import Logger
import os, boto3, time, json


logger = Logger(service="redhiftAnalysisTable", level="DEBUG")
rs = boto3.client("redshift-data")

REDSHIFT_WORKGROUP = os.environ["REDSHIFT_WORKGROUP"]
REDSHIFT_DATABASENAME = os.environ["REDSHIFT_DATABASENAME"]


def run_query_from_file(path):
    logger.debug(path)
    with open(path, "r") as f:
        sql = f.read()
        logger.debug(sql)
        result = rs.execute_statement(Database=REDSHIFT_DATABASENAME, Sql=sql, WorkgroupName=REDSHIFT_WORKGROUP)
        logger.debug(result)
        
        id = result['Id']
        statement = ''
        status = ''
        while status != 'FINISHED' and status != 'FAILED' and status != 'ABORTED':
            statement = rs.describe_statement(Id=id)
            status = statement['Status']
            logger.debug("Status:", status)
            time.sleep(5)
        logger.debug('End query: ',json.dumps(statement, indent=4, default=str))
        logger.debug(json.dumps(statement, indent=4, default=str))
        
        
        try:
            statement = rs.get_statement_result(Id=id)
            logger.debug(json.dumps(statement, indent=4, default=str))
            logger.debug(json.dumps(statement, indent=4, default=str))
        except Exception as e:
            logger.error(e)


@logger.inject_lambda_context
def handler(event, context):
    logger.debug(event)
    run_query_from_file("loadscripts/make_bi_table.sql")
    logger.debug("Done Make BI Table !!")
    logger.debug('End loadscripts/make_bi_table.sql')

