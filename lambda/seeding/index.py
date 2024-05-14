from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities import parameters
import os
import json
import pymysql


DB_SECRET_NAME = os.environ["DB_SECRET_NAME"]
DB_HOSTNAME = os.environ["DB_HOSTNAME"]
DB_PORT = int(os.environ["DB_PORT"])
DB_NAME = os.environ["DB_NAME"]
# サンプル DB のテーブルリスト
TABLE_LIST = ['products_master','orders_master','order_details_master']

secret = json.loads(parameters.get_secret(DB_SECRET_NAME, max_age=60))
logger = Logger(service="seed")

def drop_and_create_database(): #本番環境では絶対に使わないでください。
    logger.info("drop and credate")

    with pymysql.connect(host=DB_HOSTNAME, db=DB_NAME, user=secret["username"], password=secret["password"],autocommit=True) as conn:
        with conn.cursor() as cur:
            # 当該DBに存在するセッションを削除
            cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME};")
            cur.execute(f"CREATE DATABASE {DB_NAME};")

def run_query_from_file(path):
    logger.info(path)
    
    try:
        conn = pymysql.connect(host=DB_HOSTNAME, db=DB_NAME, user=secret["username"], passwd=secret["password"],autocommit=True)
        logger.info("READ MySQL")
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)

    with open(path, "r") as f:
        sql = f.read()
        logger.info(sql)
    with conn.cursor() as cur:
        cur.execute(sql)
        logger.info("Done: ", path)
    conn.close()

@logger.inject_lambda_context
def handler(event, context):
    logger.debug(event)
    command = event.get("command")

    if command is None:
        logger.info('please send events such as {"command": "init"|"seed"}')
        return

    if command == "init":
        drop_and_create_database()

    if command == "init":
        for table in TABLE_LIST:
            run_query_from_file(f"schema/create_{table}.sql")

    if command == "seed":
        for table in TABLE_LIST:
            run_query_from_file(f"seed/insert_{table}.sql")

