import os
import pyodbc
try:
    import snowflake.connector as snow
except ImportError:
    snow = None

def mssql(server, database, username=None, password=None, **kwargs):
    connectors = ["Driver={ODBC Driver 17 for SQL Server}"]
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
    def snowflake(server="global.eu-west-1", database="warehouses", username=None, password=None, role="reader_all", warehouse="training"):
        return snow.connect(
            user=username or os.environ["DBT_PROFILES_USER"],
            password=password or os.environ["DBT_PROFILES_PASSWORD"],
            account=server,
            database=database,
            warehouse=warehouse,
            role=role
        )
