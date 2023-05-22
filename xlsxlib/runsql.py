import os, sys
import collections
from concurrent import futures
import csv
import itertools
import logging

import snowflake.connector

from . import connections

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.DEBUG)

SNOWFLAKE_ACCOUNT = "global.eu-west-1"
SNOWFLAKE_WAREHOUSE = "DWH_DEV_SMALL"
SNOWFLAKE_ROLE = "dev_engineer"

SnowflakeInfo = collections.namedtuple(
    "SnowflakeInfo",
    ["user", "password", "account", "warehouse", "role", "database"]
)

#
# Spoke to Hal:
# - File format: two columns (tab/comma-separated) where the first column
#   is a batch/grouping identifier; the rest is the SQL to run
# - Probably no need for a Snowflake database; HM to check
# -
#

def get_query_batches(filepath):
    with open(filepath) as f:
        reader = csv.reader(f)
        headers = next(reader)
        if len(headers) == 1:
            rows = [["all"] + row for row in reader]
        else:
            rows = [row[:2] for row in reader]

    batches = {}
    for batch, lines in itertools.groupby(rows, lambda x: x[0]):
        batches[batch] = [line for (batch, line) in lines]
    return batches

def run_concurrent_sql(args):
    snowflake_info, batch_id, lines = args
    logger.info("Running %d lines for batch %s", len(lines), batch_id)
    db = snowflake.connector.connect(
        user=snowflake_info.user,
        password=snowflake_info.password,
        account=snowflake_info.account,
        warehouse=snowflake_info.warehouse,
        database=snowflake_info.database
    )
    logger.info("Connected to Snowflake %s with warehouse %s", snowflake_info.account, snowflake_info.warehouse)

    q = db.cursor()
    try:
        q.executemany(lines)
    finally:
        q.close()

    return "%d lines run for batch %s" % (len(lines), batch_id)

def run_batches(snowflake_info, batches):
    #
    # Initially do this naively, running one batch after another
    # Later, we can run in parallel using
    #
    concurrent_batches = [(snowflake_info, batch_id, lines) for (batch_id, lines) in batches.items()]
    with futures.ProcessPoolExecutor(max_workers=4) as executor:
        for result in executor.map(run_sql, concurrent_batches):
            logger.debug(result)

def main(args):
    sql_filename = args.sql_filename
    #
    # Attempt to read the query batches from the file first:
    # if that fails we don't need to bother opening the db connection
    #
    batches = get_query_batches(filepath)

    snowflake_info = SnowflakeInfo(
        args.snowflake_user or os.environ['DBT_PROFILES_USER'],
        args.snowflake_password or os.environ['DBT_PROFILES_PASSWORD'],
        args.snowflake_account or SNOWFLAKE_ACCOUNT,
        args.snowflake_warehouse or SNOWFLAKE_WAREHOUSE,
        args.snowflake_role or SNOWFLAKE_ROLE,
        args.snowflake_database
    )

    run_batches(snowflake_info, batches)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="A file containing SQL statements to run, optionally preceded by a batch id")
    parser.add_argument("--snowflake_user", help="Snowflake username")
    parser.add_argument("--snowflake_password", help="Snowflake password")
    parser.add_argument("--snowflake_account", help="Snowflake account")
    parser.add_argument("--snowflake_warehouse", help="Snowflake warehouse")
    parser.add_argument("--snowflake_role", help="Snowflake role")
    parser.add_argument("--snowflake_database", help="Snowflake database")
    args = parser.parse_args()

    main(args)
