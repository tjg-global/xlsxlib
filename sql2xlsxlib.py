import os, sys

import xlsxlib

class x_sql2xlsxlib (Exception): pass


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

def cursor_data(cursor):
    """cursor_data - return the current data for this cursor and whether any more is expected

    Parameters:
        cursor - an open cursor

    Yields:
        sheet_name, headers, row-generator

    Notes:
        Since some of the results could be None due to DML statements, ignore them;
        they are characterised by having no description (because no returnable values).
    """
    more_data = True
    while more_data:
        # Skip over non-DQL
        while more_data and not cursor.description:
            more_data = cursor.nextset()

        # Fetch sheet name as single-line query
        if more_data:
            sheet_name = cursor.fetchone()[0]
            more_data = cursor.nextset()

        # Skip over non-DQL
        while more_data and not cursor.description:
            more_data = cursor.nextset ()

        if more_data:
            yield sheet_name, [d[0:2] for d in cursor.description], rows(cursor, 1000)
            more_data = cursor.nextset ()

def query2xlsx(db, query, spreadsheet_filepath):
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
    query = query.replace ("GO\n", "--GO\n")

    q = db.cursor ()
    q.execute("SET NOCOUNT ON")
    q.execute("SET ANSI_WARNINGS ON")
    q.execute("SET ANSI_NULLS ON")
    q.execute(query)
    for info in xlsxlib.xlsx(cursor_data(q), spreadsheet_filepath):
        yield info
    q.close ()

def table2xlsx (db, table, spreadsheet_filepath=None):
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

def sp2xlsx (db, stored_procedure, spreadsheet_filepath):
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

def script2xlsx (db, script_filepath, spreadsheet_filepath=None):
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
    for info in query2xlsx(db, open (script_filepath).read (), spreadsheet_filepath):
        yield info
