import os, sys

from . import xlsxlib
from . import dialects

class x_sql2xlsxlib(Exception): pass

def rows(cursor, arraysize=-1):
    """Generate rows using an optional arrayfetch size
    """
    while True:
        remaining_rows = cursor.fetchmany(arraysize)
        if remaining_rows:
            for row in remaining_rows:
                yield row
        else:
            break

def query2xlsx(db, query, spreadsheet_filepath, driver, params=()):
    """query2xl - Convert the output from a query to a spreadsheet

    Parameters:
        db - open MSSQL database connection
        query - name of a query against the database
        spreadsheet_filepath - full path to the output spreadsheet

    Notes:
        The cursor description resulting from executing the query
        will be used as the header row of the spreadsheet and highlighted in bold.
        The whole spreadsheet will be have its column widths autofitted.
    """

    '''
    if True:#Later to change to SQL server
        dialect.SQL_server.pre_query()
        for info in xlsxlib.xlsx(dialect.SQL_server.cursor_data(q), spreadsheet_filepath):
        yield info
        q.close()
    '''

    '''
    snowflake_db = dialect.Snowflake(db)

    snowflake_db.pre_query()

    for info in xlsxlib.xlsx(snowflake_db.cursor_data(query), spreadsheet_filepath):
        yield info
    '''
    try:
        db_dialect = dialects.dialect_from_driver(driver)
    except KeyError:
        raise RuntimeError("Could not determine dialect from driver %s" % driver)

    dialect_db = db_dialect(db)
    query = dialect_db.preprocess(query, params)
    dialect_db.pre_query()
    for info in xlsxlib.xlsx(dialect_db.cursor_data(query), spreadsheet_filepath):
        yield info


def table2xlsx(db, table, spreadsheet_filepath=None, dialect=None):
    """table2xlsx - Convert the contents of a table to a spreadsheet

    Parameters:
        db - open MSSQL database connection
        table - name of a table or view in the database
        spreadsheet_filepath - full path to the output spreadsheet

    Notes:
        The table/view will be queried using SELECT * FROM <table> and
        the description of this cursor will form the header row.
    """
    if spreadsheet_filepath is None:
        spreadsheet_filepath = "%s.xlsx" % (table,)
    for info in query2xlsx(db, "SELECT '%s'\nSELECT * FROM %s" % (table, table,), spreadsheet_filepath):
        yield info


def sp2xlsx(db, stored_procedure, spreadsheet_filepath):
    """sql2xlsx - Convert the output from a stored procedure to a spreadsheet

    Parameters:
        db - open MSSQL database connection
        stored_procedure - name of a stored procedure in the database
        spreadsheet_filepath - full path to the output spreadsheet

    Notes:
        The stored procedure is expected to return a cursor. The cursor description
        will be used as the header row of the spreadsheet and highlighted in bold.
        The whole spreadsheet will be have its column widths autofitted.
    """
    for info in query2xlsx(db, stored_procedure, spreadsheet_filepath):
        yield info


def script2xlsx(db, script_filepath, spreadsheet_filepath=None):
    """script2xlsx - Convert the output(s) from a script to a spreadsheet

    Parameters:
        db - open MSSQL database connection
        script - name of a script file (typically foo.sql)
        spreadsheet_filepath - (optional) full path to the output spreadsheet

    Notes:
        The cursor description resulting from executing the query
        will be used as the header row of the spreadsheet and highlighted in bold.
        The whole spreadsheet will be have its column widths autofitted.
    """
    if spreadsheet_filepath is None:
        base, ext = os.path.splitext(os.path.basename(script_filepath))
        spreadsheet_filepath = base + ".xlsx"
    for info in query2xlsx(db, open(script_filepath).read(), spreadsheet_filepath):
        yield info
