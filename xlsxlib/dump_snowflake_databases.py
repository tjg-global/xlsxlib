"""Run over all databases in a Snowflake instance and produce DDL files for each object
"""
import os, sys
import argparse
import collections
from concurrent import futures
import csv
import itertools
import logging
import shutil

import snowflake.connector

from . import dump_database

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.DEBUG)

stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.INFO)
logger.addHandler(stdout_handler)

log_file_path = "dump_snowflake_databases.log"
log_format = "%(name)s - %(asctime)s - %(levelname)s - %(message)s"
file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
file_handler.setFormatter(logging.Formatter(log_format))
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

error_file_path = "dump_snowflake_databases.errors.log"
error_handler = logging.FileHandler(error_file_path)
error_handler.setFormatter(logging.Formatter(log_format))
error_handler.setLevel(logging.WARN)
logger.addHandler(error_handler)

SNOWFLAKE_ACCOUNT = "global.eu-west-1"
SNOWFLAKE_WAREHOUSE = "dwh_etl_xsmall"
SNOWFLAKE_ROLE = "ACCOUNTADMIN"
SNOWFLAKE_DATABASE = "snowflake"

SnowflakeInfo = collections.namedtuple(
    "SnowflakeInfo",
    ["user", "password", "account", "warehouse", "role", "database"]
)

def dump_normal_databases(database_name, args, q):
    q.execute("SELECT GET_DDL('database', %s, true);", [database_name])
    [ddl] = q.fetchone()
    if args.debug:
        with open("%s.sql" % database_name, "w", encoding="utf-8") as f:
            f.write(ddl)
    dump_database.dump_database(database_name, ddl, args.debug, logger=logger)

def dump_imported_databases(database_name):
    dump_database.dump_imported_database(database_name, logger=logger)

def run(args):
    #
    # If we're running for all databases, clear all files first
    # so we get a clear view of deleted databases
    #
    if not args.name_pattern:
        dump_database.remove_existing_files(logger=logger)

    cwd = os.getcwd()

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
    name_pattern = args.name_pattern or '%'
    try:
        q = db.cursor()
        try:
            database_sql = "SELECT database_name, type FROM INFORMATION_SCHEMA.DATABASES WHERE database_name ILIKE %s ORDER BY database_name;"
            q.execute(database_sql, [name_pattern])
            for row in q.fetchall():
                [database_name, type] = row
                logger.info("DATABASE: %s - %s", database_name, type)

                if args.by_database:
                    #
                    # Ensure we're starting with an empty database folder
                    # so that removal diffs are honoured
                    #
                    if os.path.exists(database_name):
                        shutil.rmtree(database_name)
                    os.mkdir(database_name)
                    os.chdir(database_name)

                if type == 'STANDARD':
                    dump_normal_databases(database_name, args, q)
                else:
                    dump_imported_databases(database_name)

                os.chdir(cwd)
        finally:
            q.close()
    finally:
        db.close()

def command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument("name_pattern", help="A SQL-wildcard pattern identifying one or more database names", nargs='?')
    parser.add_argument("--snowflake_user", help="Snowflake username")
    parser.add_argument("--snowflake_password", help="Snowflake password")
    parser.add_argument("--snowflake_account", help="Snowflake account")
    parser.add_argument("--snowflake_warehouse", help="Snowflake warehouse")
    parser.add_argument("--snowflake_role", help="Snowflake role")
    parser.add_argument("--by-database", help="Create a folder per database", action='store_true')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--no-debug', dest='debug', action='store_false')
    parser.set_defaults(debug=False, by_database=True)
    args = parser.parse_args()
    run(args)

if __name__ == '__main__':
    command_line()
