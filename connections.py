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
    SNOWFLAKE_SERVER = "global.eu-west-1"
    SNOWFLAKE_DATABASE = "warehouses"
    SNOWFLAKE_ROLE = "reader_all"
    SNOWFLAKE_WAREHOUSE = "training"
    def snowflake(server=SNOWFLAKE_SERVER, database=SNOWFLAKE_WAREHOUSE, username=None, password=None, role=SNOWFLAKE_ROLE, warehouse=SNOWFLAKE_WAREHOUSE):
        return snow.connect(
            user=username or os.environ["DBT_PROFILES_USER"],
            password=password or os.environ["DBT_PROFILES_PASSWORD"],
            account=server or SNOWFLAKE_SERVER,
            database=database or SNOWFLAKE_DATABASE,
            warehouse=warehouse or SNOWFLAKE_WAREHOUSE,
            role=role or SNOWFLAKE_ROLE
        )
