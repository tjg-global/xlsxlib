import os, sys
import argparse
import collections
from concurrent import futures
import csv
import itertools
import logging

import snowflake.connector

from . import connections

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.DEBUG)
stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.DEBUG)
logger.addHandler(stdout_handler)


SNOWFLAKE_ACCOUNT = "global.eu-west-1"
SNOWFLAKE_WAREHOUSE = "DWH_DEV_SMALL"
SNOWFLAKE_ROLE = "dev_engineer"

SnowflakeInfo = collections.namedtuple(
    "SnowflakeInfo",
    ["user", "password", "account", "warehouse", "role", "database"]
)

def get_query_batches(filepath):
    """Read batches of SQL commands from a file

    If the file simply contains SQL with no batching column, treat them
    all as one batch. If there is an initial column, treat is as a batch
    and run the different batches concurrently.
    """
    logger.info("Read batches from %s", filepath)
    with open(filepath) as f:
        reader = csv.reader(f)
        headers = next(reader)
        if len(headers) == 1:
            logger.warning("No batch column found")
            rows = [["all"] + row for row in reader]
        else:
            rows = [row[:2] for row in reader]

    batches = collections.defaultdict(list)
    for batch, line in rows:
        if batch not in ("xx"):
            batches[batch].append(line)
    logger.info("Found %d batches", len(batches))
    return batches

def run_lines(batch_id, db, lines):
    q = db.cursor()
    try:
        print("DUMMY: %d lines of SQL" % len(lines))
        #~ q.executemany(lines)
    finally:
        q.close()

def run_epilog(batch_id, db, epilog):
    q = db.cursor()
    try:
        q.execute(epilog)
        rows = q.fetchall()
        if rows:
            logger.info("Results for batch %s", batch_id)
            logger.info("-" * 100)
            for row in rows:
                logger.info(" | ".join("%s" % i for i in row))
            logger.info("-" * 100)
    finally:
        q.close()

def run_sql(args):
    snowflake_info, batch_id, all_lines = args
    if not all_lines:
        return "Nothing to do for batch %s" % batch_id
    lines, epilog = all_lines[:-1], all_lines[-1]
    logger.info("Running %d lines for batch %s", len(lines), batch_id)
    db = snowflake.connector.connect(
        user=snowflake_info.user,
        password=snowflake_info.password,
        account=snowflake_info.account,
        warehouse=snowflake_info.warehouse,
        database=snowflake_info.database
    )
    try:
        if lines:
            run_lines(batch_id, db, epilog)
        if epilog.strip():
            run_epilog(batch_id, db, epilog)
        return "%d lines run for batch %s" % (len(lines), batch_id)
    finally:
        db.close()

def run_batches(snowflake_info, batches):
    #
    # Initially do this naively, running one batch after another
    # Later, we can run in parallel using
    #
    concurrent_batches = [(snowflake_info, batch_id, lines) for (batch_id, lines) in batches.items()]
    with futures.ProcessPoolExecutor(max_workers=4) as executor:
        for result in executor.map(run_sql, concurrent_batches):
            logger.info(result)

def run(args):
    #
    # Attempt to read the query batches from the file first:
    # if that fails we don't need to bother opening the db connection
    #
    batches = get_query_batches(args.filename)

    snowflake_info = SnowflakeInfo(
        args.snowflake_user or os.environ['DBT_PROFILES_USER'],
        args.snowflake_password or os.environ['DBT_PROFILES_PASSWORD'],
        args.snowflake_account or SNOWFLAKE_ACCOUNT,
        args.snowflake_warehouse or SNOWFLAKE_WAREHOUSE,
        args.snowflake_role or SNOWFLAKE_ROLE,
        args.snowflake_database
    )

    run_batches(snowflake_info, batches)

def command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="A file containing SQL statements to run, optionally preceded by a batch id")
    parser.add_argument("--snowflake_user", help="Snowflake username")
    parser.add_argument("--snowflake_password", help="Snowflake password")
    parser.add_argument("--snowflake_account", help="Snowflake account")
    parser.add_argument("--snowflake_warehouse", help="Snowflake warehouse")
    parser.add_argument("--snowflake_role", help="Snowflake role")
    parser.add_argument("--snowflake_database", help="Snowflake database")
    args = parser.parse_args()
    run(args)

if __name__ == '__main__':
    command_line()
