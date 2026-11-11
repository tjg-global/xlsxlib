import os
import re
import urllib.parse

import pyodbc
try:
    import snowflake.connector as snow
except ImportError:
    snow = None

def parse_dburi_ex(dburi):
    if not re.match("[^:]+://", dburi):
        dburi = "mssql://" + dburi
    parsed = urllib.parse.urlparse(dburi)
    return (
        parsed.scheme or "mssql",
        parsed.hostname or "",
        (parsed.path or "").lstrip("/"),
        parsed.username or "",
        parsed.password or "",
    )

def _mssql_driver():
    """Return (version, name) of the highest "ODBC Driver NN for SQL Server" installed."""
    candidates = []
    for name in pyodbc.drivers():
        m = re.match(r"ODBC Driver (\d+) for SQL Server$", name)
        if m:
            candidates.append((int(m.group(1)), name))
    if not candidates:
        raise RuntimeError(
            "No 'ODBC Driver NN for SQL Server' installed; found: %s" % pyodbc.drivers()
        )
    return max(candidates)

def mssql(server, database, username=None, password=None, trust_server_certificate=None, **kwargs):
    """Connect to SQL Server using the newest installed MS ODBC driver.

    trust_server_certificate: True/False to set TrustServerCertificate explicitly.
    None (default) turns it on for Driver 18+, which encrypts by default, so that
    servers with self-signed certificates keep working as they did under Driver 17.
    """
    version, driver = _mssql_driver()
    connectors = ["Driver={%s}" % driver]
    if trust_server_certificate is None:
        trust_server_certificate = version >= 18
    if trust_server_certificate:
        connectors.append("TrustServerCertificate=yes")
    connectors.append("Server=%s" % server)
    connectors.append("Database=%s" % database)
    if username:
        connectors.append("UID=%s" % username)
        connectors.append("PWD=%s" % password)
    else:
        connectors.append("Trusted_Connection=Yes")
    return pyodbc.connect(";".join(connectors), **kwargs)

def mysql(server, database, username, password, **kwargs):
    return pyodbc.connect(
        "DRIVER={MySQL ODBC 5.3 Unicode Driver};OPTION=3;SERVER=%s;DATABASE=%s;USER=%s;PASSWORD=%s;"
        % (server, database, username, password),
        **kwargs
    )

if snow:
    SNOWFLAKE_SERVER = "global.eu-west-1"
    SNOWFLAKE_DATABASE = "warehouses"
    SNOWFLAKE_ROLE = "view_all"
    SNOWFLAKE_WAREHOUSE = "view_xsmall"
    def snowflake(server=SNOWFLAKE_SERVER, database=SNOWFLAKE_WAREHOUSE, username=None, password=None, role=SNOWFLAKE_ROLE, warehouse=SNOWFLAKE_WAREHOUSE):
        return snow.connect(
            user=username or os.environ["DBT_PROFILES_USER"],
            private_key_file=os.environ["DBT_KEY_PATH"] or os.environ.get("PRIVATE_KEY_PATH"),
            private_key_file_pwd=os.environ.get("DBT_KEY_PASSPHRASE") or os.environ.get("PRIVATE_KEY_PASSPHRASE"),
            account=server or SNOWFLAKE_SERVER,
            database=database or SNOWFLAKE_DATABASE,
            warehouse=warehouse or SNOWFLAKE_WAREHOUSE,
            role=role or SNOWFLAKE_ROLE
        )
