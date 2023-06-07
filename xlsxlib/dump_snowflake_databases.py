import os, sys
import argparse
import collections
from concurrent import futures
import csv
import itertools
import logging

import snowflake.connector

from . import connections
from . import dump_database

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.DEBUG)
stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.DEBUG)
logger.addHandler(stdout_handler)


SNOWFLAKE_ACCOUNT = "global.eu-west-1"
SNOWFLAKE_WAREHOUSE = "DWH_ETL_XSMALL"
SNOWFLAKE_ROLE = "architect"
SNOWFLAKE_DATABASE = "snowflake"

SnowflakeInfo = collections.namedtuple(
    "SnowflakeInfo",
    ["user", "password", "account", "warehouse", "role", "database"]
)

def run(args):
    snowflake_info = SnowflakeInfo(
        args.snowflake_user or os.environ['DBT_PROFILES_USER'],
        args.snowflake_password or os.environ['DBT_PROFILES_PASSWORD'],
        args.snowflake_account or SNOWFLAKE_ACCOUNT,
        args.snowflake_warehouse or SNOWFLAKE_WAREHOUSE,
        args.snowflake_role or SNOWFLAKE_ROLE,
        SNOWFLAKE_DATABASE
    )
    db = snowflake.connector.connect(
        user=snowflake_info.user,
        password=snowflake_info.password,
        account=snowflake_info.account,
        warehouse=snowflake_info.warehouse,
        database=snowflake_info.database,
        role=snowflake_info.role
    )
    try:
        q = db.cursor()
        try:
            q.execute("SELECT database_name FROM INFORMATION_SCHEMA.DATABASES LIMIT 10;")
            for row in q.fetchall():
                database_name = row[0]
                print()
                print(database_name)
                q.execute("SELECT GET_DDL('database', %s, true);", [database_name])
                ddl = q.fetchone()[0]
                with open("%s.sql" % database_name, "w") as f:
                    f.write(ddl)
                dump_database.dump_database(database_name, ddl)
        finally:
            q.close()
    finally:
        db.close()

def command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snowflake_user", help="Snowflake username")
    parser.add_argument("--snowflake_password", help="Snowflake password")
    parser.add_argument("--snowflake_account", help="Snowflake account")
    parser.add_argument("--snowflake_warehouse", help="Snowflake warehouse")
    parser.add_argument("--snowflake_role", help="Snowflake role")
    args = parser.parse_args()
    run(args)

if __name__ == '__main__':
    command_line()
