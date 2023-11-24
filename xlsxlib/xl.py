#!python3
import os, sys
import logging
logging.basicConfig(level=logging.INFO)
import re
import traceback
import urllib.parse as urlparse

from . import sql2xlsxlib
from . import connections

DEFAULT_DATABASE = "SVR09/TDI"

def from_code(code):
    return " ".join(code.split("_")).title()

def munge_script_for_mssql(query, params):
    query = re.sub(r"USE\s+.*", "", query)
    query = re.sub(r"\bGO\b", "", query)

    vars = re.findall(r"%\((\w+)\)s", query)
    values = {}
    for index, var in enumerate(vars):
        try:
            values[var] = params[index]
        except IndexError:
            if var not in values:
                values[var] = input("%s: " % from_code(var))
    if values:
        for k, v in values.items():
            logging.info("%s => %s" % (k, v))
        query = query % values

    return query

def main(script_filepath, database=None, xls_filepath=None, *params):
    if database is None:
        database = input("Database [%s]: " % DEFAULT_DATABASE) or DEFAULT_DATABASE

    #
    # Ignore .SQL (or any other extension)
    #
    head, tail = os.path.split(script_filepath)
    basename, ext = os.path.splitext(os.path.basename(script_filepath))

    path = head or os.getcwd()
    if xls_filepath is None:
        xls_filepath = os.path.join(basename + ".xlsx")
    xls_filepath = os.path.abspath(xls_filepath)
    print("Write to", xls_filepath)

    query = open(script_filepath).read()
    #
    # If we haven't specified a database and the script has a USE
    # statement, use the database in that USE statement (and then
    # strip it out!). NB only do this for MSSQL
    #
    driver, server_name, database_name, username, password = connections.parse_dburi_ex(database)
    print("Driver is", driver)
    if driver == "mssql":
        if not database_name:
            print("Find database for mssql")
            for database_name in re.findall("USE\s+(.*)", query):
                #~ database = (database.rstrip("/")) + "/" + database_name
                break
        query = munge_script_for_mssql(query, params)
    elif driver == "snowflake":
        #
        # The most common configuration for Snowflake will be the database
        # name alone (as opposed to mssql where it will be the server name alone)
        #
        if server_name and not database_name:
            print("Swap server & database for Snowflake")
            server_name, database_name = database_name, server_name

    if driver == "mssql":
        db = connections.mssql(server_name, database_name, username, password)
    elif driver == "snowflake":
        db = connections.snowflake(server_name, database_name, username, password)
    else:
        raise RuntimeError("Unknown database type %s" % driver)

    logging.info("Writing to %s ...", xls_filepath)
    if os.path.isfile(xls_filepath):
        os.remove(xls_filepath)
    for info in sql2xlsxlib.query2xlsx(
        db=db, query=query, spreadsheet_filepath=xls_filepath, driver=driver
    ):
        logging.info(info)

    #
    # Assume that if any parameters were passed, we're in
    # "batch" mode and won't want to open the spreadsheet.
    #
    if not params:
        os.startfile(xls_filepath)

def command_line():
    #
    # If no extra params are supplied, dump out a useful help string
    #
    if len(sys.argv) == 1:
        print("%s sql_filepath dburi [xls_filepath [params...]]" % sys.argv[0])
    else:
        main(*sys.argv[1:])

if __name__ == "__main__":
    command_line()
