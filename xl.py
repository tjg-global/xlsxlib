#!python3
import os, sys
import sql
import sql2xlsxlib
import re
import traceback

import logging

logging.basicConfig(level=logging.INFO)
import sql

try:
    raw_input
except NameError:
    raw_input = input

DEFAULT_DATABASE = "SVR-DB-CAS-DEV/TDI"

DEFAULT_DATABASES = {
    "TDI": "SVR09",
    "STAGING": "SVR-DWSQLPRD",
    "WALRUS": "SVR-DWSQLPRD",
}


def from_code(code):
    return " ".join(code.split("_")).title()


def main(script_filepath, database=None, xls_filepath=None, *params):
    if database is None:
        database = raw_input("Database [%s]: " % DEFAULT_DATABASE) or DEFAULT_DATABASE

    #
    # Ignore .SQL (or any other extension)
    #
    head, tail = os.path.split(script_filepath)
    basename, ext = os.path.splitext(os.path.basename(script_filepath))

    path = head or os.getcwd()
    if xls_filepath is None:
        xls_filepath = os.path.join(basename + ".xlsx")
    xls_filepath = os.path.abspath(xls_filepath)

    query = open(script_filepath).read()
    #
    # If we haven't specified a database and the script has a USE
    # statement, use the database in that USE statement (and then
    # strip it out!).
    #
    driver, _, database_name, _, _ = sql.parse_dburi_ex(database)
    if not database_name:
        for database_name in re.findall("USE\s+(.*)", query):
            database = (database.rstrip("/")) + "/" + database_name
            break
    query = re.sub(r"USE\s+.*", "", query)
    query = re.sub(r"GO\s*", "", query)

    vars = re.findall(r"%\((\w+)\)s", query)
    values = {}
    for index, var in enumerate(vars):
        try:
            values[var] = params[index]
        except IndexError:
            if var not in values:
                values[var] = raw_input("%s: " % from_code(var))
    if values:
        for k, v in values.items():
            logging.info("%s => %s" % (k, v))
        query = query % values
    logging.info("Writing to %s ...", xls_filepath)
    if os.path.isfile(xls_filepath):
        os.remove(xls_filepath)
    db, driver = sql.database_ex(database)
    if driver == "mssql":
        with db.cursor() as q:
            q.execute("SET ANSI_WARNINGS ON\nSET ANSI_NULLS ON\n")
        try:
            db.adoConn.CommandTimeout = 600
        except AttributeError:
            pass
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


if __name__ == "__main__":
    #
    # If no extra params are supplied, dump out a useful help string
    #
    if len(sys.argv) == 1:
        print("%s sql_filepath dburi [xls_filepath [params...]]" % sys.argv[0])
    else:
        main(*sys.argv[1:])
