import os, sys
import csv
import logging
import re
import urllib.parse as urlparse

import pyodbc

Connection = pyodbc.Connection
try:
    import snowflake.connector as snowflake
except ImportError:
    snowflake = None

import win32api
import win32con
import win32net
import win32netcon

import row


class sql_exception(Exception):
    pass


class sql_unimplemented(sql_exception):
    pass


class sql_could_not_connect(sql_exception):
    pass


log = logging.getLogger("sql")

DEFAULT_DATABASES = {}
DATABASE_STARTUP = {}

class Database(object):

    def __init__(self, dbtype, connection):
        self.type = dbtype
        self._connection = connection

    def __repr__(self):
        return "<%s database connection %s>" % (self.type, self._connection)

    def __getattr__(self, attr):
        return getattr(self._connection, attr)



def mysql_connection(server, database, username, password):
    return pyodbc.connect(
        "DRIVER={MySQL ODBC 5.3 Unicode Driver};OPTION=3;SERVER=%s;DATABASE=%s;USER=%s;PASSWORD=%s;"
        % (server, database, username, password)
    )


def snowflake_connection(database="warehouses", username=None, password=None):
    return snowflake.connect(
        user=username or os.environ["DBT_PROFILES_USER"],
        password=password or os.environ["DBT_PROFILES_PASSWORD"],
        account="global.eu-west-1",
        database=database,
        warehouse="team_technology",
        role="sr_engineer",
    )


def database(database_name):
    try:
        server, database, username, password = DEFAULT_DATABASES[database_name]
    except KeyError:
        server, database, username, password = parse_dburi(database_name)

    db = pyodbc_connection(server, database, username, password)
    startup = DATABASE_STARTUP.get(database_name)
    if startup:
        q = db.cursor()
        q.execute(startup)
        q.close()
    return db


def database_ex(database_name):
    driver, server, database, username, password = parse_dburi_ex(database_name)
    if driver == "mssql":
        return Database("mssql", pyodbc_connection(server, database, username, password))
    elif driver == "mysql":
        return Database("mysql", mysql_connection(server, database, username, password))
    elif driver == "snowflake":
        return Database("snowflake", snowflake_connection(database, username, password))
    else:
        raise RuntimeError("Unknown driver: %s" % driver)


ERROR = "User %s is not allowed to access database %s.%s"


def pyodbc_connection(server, database, username="", password="", **kwargs):
    connectors = ["Driver={ODBC Driver 17 for SQL Server}"]
    connectors.append("Server=%s" % server)
    connectors.append("Database=%s" % database)
    if username:
        connectors.append("UID=%s" % username)
        connectors.append("PWD=%s" % password)
    else:
        connectors.append("Trusted_Connection=Yes")
    try:
        return pyodbc.connect(";".join(connectors), **kwargs)
    except pyodbc.Error as info:
        code, message = info
        if code == "28000":
            win32api.MessageBox(
                0,
                ERROR
                % (
                    username or win32api.GetUserNameEx(win32con.NameSamCompatible),
                    server,
                    database,
                ),
                "Problem connecting to %s.%s" % (server, database),
            )
            raise sql_could_not_connect
        else:
            raise


def parse_dburi(dburi):
    if not dburi.startswith("mssql://"):
        dburi = "mssql://" + dburi
    parsed = urlparse.urlparse(dburi)
    return (
        parsed.hostname or "",
        (parsed.path or "").lstrip("/"),
        parsed.username or "",
        parsed.password or "",
    )


def parse_dburi_ex(dburi):
    if not re.match("[^:]+://", dburi):
        dburi = "mssql://" + dburi
    parsed = urlparse.urlparse(dburi)
    return (
        parsed.scheme or "mssql",
        parsed.hostname or "",
        (parsed.path or "").lstrip("/"),
        parsed.username or "",
        parsed.password or "",
    )


def connection_from_oledb_dsn(oledb_dsn, **kwargs):
    """An OLEDB DSN looks something like this:

    Data Source=SERVER;User ID=USERNAME;Password=PWD;Initial Catalog=DBNAME;Provider=SQLOLEDB.1
    Data Source=SVR-DWSQLDEV;Integrated Security=SSPI;Initial Catalog=WALRUS;Provider=SQLOLEDB.1;Persist Security Info=True;

    Parse this data and extract the elements we need pass to pyodbc_connection (along with
    any arbitrary keyword args)
    """
    info = {}
    items = (i.strip() for i in oledb_dsn.split(";") if i.strip())
    for item in items:
        k, v = item.split("=")
        info[k.lower()] = v
    if "data source" not in info:
        raise RuntimeError("No data source supplied in DSN %s" % oledb_dsn)
    else:
        server = info["data source"]
    database = info.get("initial catalog", "")
    username = info.get("user id", "")
    password = info.get("password", "")
    return pyodbc_connection(server, database, username, password, **kwargs)


def servers(domain=None):
    hResume = 0
    while True:
        info, total, hResume = win32net.NetServerEnum(
            None, 100, win32netcon.SV_TYPE_SQLSERVER, domain, hResume
        )
        for server in info:
            yield server["name"]
        if hResume == 0:
            break

    def get_credentials(server="", database="", username="", password=""):
        from winsys import dialogs

        if isinstance(server, list):
            possible_servers = server
        else:
            possible_servers = sorted(s for s in servers() if s != server)
            if server:
                possible_servers = [server] + possible_servers
        return dialogs.dialog(
            "Database Logon",
            ("Server", possible_servers),
            ("Database", database or ""),
            ("Username", username or ""),
            ("Password", password or ""),
        )


def query_to_csv(db, query, csv_filename):
    header, rows = fetch_query(db, query, with_headers=1)
    write_csv(csv_filename, rows, header)


def fetchall(q):
    """fetchall - return a described list of rows from a cursor resultset

    Uses dtuple to return a more convenient version of a cursor's resultset.
    Rather than struggling with tuple indices, you can reference the rows
     as a dictionary or even as a class.

    Example:
      import sql
      q = db.cursor ()
      q.execute ("SELECT * FROM wb_booking_types")
      booking_types = sql.fetchall (q)
      for booking_type in booking_types:
        print "id = %(id)d, code = %(booking_type_code)s, name = %(booking_type_name)s, active = %(active)d" % booking_type.asMapping ()
      q.close ()
    """
    Row = row.Row([d[0] for d in q.description])
    return [Row(i) for i in q.fetchall()]


def fetchone(q):
    """fetchone - return one described row from a cursor resultset

    Uses dtuple to return a more convenient version of a cursor's resultset.
    Rather than struggling with tuple indices, you can reference the rows
     as a dictionary or even as a class.

    Example:
      import sql
      q = db.cursor ()
      q.execute ("SELECT * FROM wb_booking_types")
      booking_type = sql.fetchone (q)
      print "id = %(id)d, code = %(booking_type_code)s, name = %(booking_type_name)s, active = %(active)d" % booking_type.asMapping ()
      q.close ()
    """
    Row = row.Row([d[0] for d in q.description])
    return Row(q.fetchone())


def fetch_value(db, q_sql):
    q = db.cursor()
    row = []

    try:
        q.execute(q_sql, ())
        row = fetchone(q)
    finally:
        q.close()

    if len(row) == 1:
        return row[0]
    else:
        raise sql_exception("fetch_value expects a one-column row")


def fetch_query(db, q_sql, params=None, with_headers=0):
    """
    fetch_query - execute a query against a database and return the resultset
      => [header,] rows
    """
    q = db.cursor()
    log.debug(
        "%s\n%s\n%s",
        ("-" * 80),
        re.sub("[\r\n]{2,}", "\n", q_sql),
        params or "<No params>",
    )
    if params is None:
        q.execute(q_sql)
    else:
        q.execute(q_sql, params)

    #
    # Bit of a fudge to avoid issues with adodbapi
    #  when the result set is empty
    #
    if q.description is None:
        rows = header = []
    else:
        rows = fetchall(q)
        header = [f[0] for f in q.description]
    q.close()

    if with_headers:
        return (header, rows)
    else:
        return rows


def fetch_queries(db, q_sql, params=None):
    """fetch_queries - fetch one or more resultsets from a database"""
    results = []

    q = db.cursor()
    if params is None:
        q.execute(q_sql)
    else:
        q.execute(q_sql, params)

    while 1:
        results.append(fetchall(q))
        if q.nextset() is None:
            break

    q.close()
    return results


def fetch_query_with_header(db, q_sql, params=None):
    "fetch_query_with_header - return the resultset and the header"
    return fetch_query(db=db, q_sql=q_sql, params=params, with_headers=1)


def execute_script(db, script, params=None, delimiter="\nGO", debug=0):
    """execute_script - take a SQL file and run it

    Splits the script into batches at the GO statements (which aren't
     part of SQL).
    Designed for DDL, so makes no attempt to return resultsets
    """
    execute_sql(
        db=db, sql=open(script).read(), params=params, delimiter=delimiter, debug=debug
    )


def execute_sql(db, sql, params=None, delimiter=r"\bGO\b", debug=0):
    """execute_sql - take a series of SQL statements and run them

    Splits the script into batches at the GO statements (which aren't
     part of SQL.
    Designed for DDL, so makes no attempt to return resultsets
    """
    re_delimiter = re.compile(delimiter, re.IGNORECASE)
    q = db.cursor()
    # split at GO on a linebreak to avoid problems with line-commented
    #  GO in a comment block.
    try:
        for s in re_delimiter.split(sql):
            try:
                if s:
                    log.debug(
                        "***%s\n%s\n%s",
                        ("-" * 80),
                        re.sub("[\r\n]{2,}", "\n", s),
                        params or "<No params>",
                    )
                    if params is None:
                        q.execute(s)
                    else:
                        q.execute(s, params)
            except:
                raise
    finally:
        q.close()


class csv_writer:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.parser = csv.parser()

    def writerow(self, row):
        self.csv_file.write("%s\n" % self.parser.join(row))



def query2csv(db, q_sql, csv_filename):
    header, rows = fetch_query_with_header(db, q_sql)
    write_csv(csv_filename, rows, header)


#
# If the item is of type (key) then convert item
# using factory (value)
#
def nuller(i):
    return None


TYPE_MAPPER = {type(True): int, type(None): nuller}


def write_csv(csv_filename, rows, headers):
    f = open(csv_filename, "wb")
    try:
        if sys.version_info[:2] < (2, 3):
            writer = csv_writer(f)
        else:
            writer = csv.writer(f)

        if headers:
            writer.writerow(headers)
        for row in rows:
            for item in row:
                writer.writerow([TYPE_MAPPER.get(type(i), type(i))(i) for i in row])

    finally:
        f.close()


def csv_reader(csv_file):
    p = csv.parser(ms_double_quote=1)
    rows = []
    while 1:
        line = csv_file.readline()
        if not line:
            break
        fields = p.parse(line)
        if not fields:
            continue
        rows.append(fields)

    return rows


def fetch_csv(csv_file, with_header=0, ignore_blanks=1):
    """
    fetch rows from a csv file as though from a database,
     optionally ignoring wholly blank lines.
    NB the first row is *always* assumed to be a header

     csv_file - name of file to be read; no assumptions
     with_header - return the header list before the data?
     ignore_blanks - ignore entirely blank lines?

     => [header,] rows
    """

    f = open(csv_file)
    try:
        if sys.version_info[:2] < (2, 3):
            reader = csv_reader(f)
        else:
            reader = csv.reader(f)

        rows = []
        first = 1
        for fields in reader:
            if first:
                header = fields
                Row = row.Row(header)
                first = 0
            else:
                total_field = "".join(field.strip() for field in fields)
                if not ignore_blanks or total_field > "":
                    try:
                        for i in range(len(fields), len(header)):
                            fields.append("")
                        rows.append(Row(fields))
                    except ValueError:
                        print("Unable to process row:")
                        pprint(fields)
                        break

    finally:
        f.close()

    if with_header:
        return (header, rows)
    else:
        return rows


#
# Shortcut class to give an object-oriented
#  feel to the data fetches
#
class sql_database:
    def __init__(self, db):
        self.db = db

    def execute(self, query, *args, **kwargs):
        return execute_sql(self.db, query, *args, **kwargs)

    execute_sql = execute

    def fetch(self, query, *args, **kwargs):
        return fetch_query(self.db, query, *args, **kwargs)

    fetch_query = fetch

    def fetch_queries(self, query, *args, **kwargs):
        return fetch_queries(self.db, query, *args, **kwargs)

    def begin(self):
        return execute_sql(self.db, "BEGIN TRANSACTION")

    def commit(self):
        return execute_sql(self.db, "COMMIT TRANSACTION")

    def rollback(self):
        return execute_sql(self.db, "ROLLBACK TRANSACTION")


class csv_database:
    def __init__(self, path="."):
        self.path = path

    def fetch(self, table, *args):
        root, ext = os.path.splitext(table)
        if not ext:
            ext = ".csv"
        return fetch_csv(os.path.join(self.path, root + ext), *args)
