import os, sys
from . import connections

SNOWFLAKE_ACCOUNT = "global.eu-west-1"
SNOWFLAKE_WAREHOUSE = "DWH_DEV_SMALL"
SNOWFLAKE_ROLE = "dev_engineer"

#
# Spoke to Hal:
# - File format: two columns (tab/comma-separated) where the first column
#   is a batch/grouping identifier; the rest is the SQL to run
# - Probably no need for a Snowflake database; HM to check
# -
#

def preprocess(query):
    #
    # Find any instance of a USE DATABASE etc. command
    # Add it to the preamble and then remove from the query
    # NB this is very naive, assuming that all the USE statements
    # are at the top of the file. If any is effectively between
    # different statements then this will fail to achieve the
    # desired effect
    #
    r_use_statement = re.compile(
        r"USE\s+(?:DATABASE|ROLE|WAREHOUSE|SCHEMA)\s+\w+;",
        flags=re.IGNORECASE
    )
    use_statements = r_use_statement.findall(query)
    preamble += "\n".join(use_statements)
    return preamble, r_use_statement.sub("", query)


def run_sql(db, sql):
    q = db.cursor()
    try:
        for statement in sql.split(";"):
            q.execute(statement)
    finally:
        q.close()

def run_preamble(db, query):
    preamble, query = preprocess(query)
    if preamble:
        run_sql(db, sql)
    return query

def main(args):
    sql_filename = args.sql_filename
    batch_size = args.batch_size or -1
    snowflake_user = args.snowflake_user or os.environ['DBT_PROFILES_USER']
    snowflake_password = args.snowflake_password or os.environ['DBT_PROFILES_PASSWORD']
    snowflake_account = args.snowflake_account or SNOWFLAKE_ACCOUNT
    snowflake_warehouse = args.snowflake_warehouse or SNOWFLAKE_WAREHOUSE
    snowflake_role = args.snowflake_role or SNOWFLAKE_ROLE
    snowflake_database = args.snowflake_database

    #
    # Attempt to read the query from the file first: if that fails
    # we don't bother opening the db connection
    #
    with open(sql_filename) as f:
        query = f.read()

    db = connections.snowflake(
        server=snowflake_account,
        database=snowflake_database,
        username=snowflake_user,
        password=snowflake_password,
        role=snowflake_role,
        warehouse=snowflake_warehouse
    )

    query = run_preamble(db, query)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("sql_filename")
    parser.add_argument("--batch_size", help="how big batch to use")
    parser.add_argument("--snowflake_user", help="Snowflake username")
    parser.add_argument("--snowflake_password", help="Snowflake password")
    parser.add_argument("--snowflake_account", help="Snowflake account")
    parser.add_argument("--snowflake_warehouse", help="Snowflake warehouse")
    parser.add_argument("--snowflake_role", help="Snowflake role")
    parser.add_argument("--snowflake_database", help="Snowflake database")
    args = parser.parse_args()

    main(args)
