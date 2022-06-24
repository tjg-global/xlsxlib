import os, sys
import sql2xlsxlib
#import pyodbc
import snowflake.connector

USERNAME = 'davide.masini@global.com'
PASSWORD = 'Y52JMaA6EciS3joKfJOz'
ACCOUNT = 'global.eu-west-1'

sf_db = snowflake.connector.connect(
    user=USERNAME,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse='DEV_DWH_ETL_XSMALL',
)

sf_db.cursor().execute('USE ROLE DEV_ENGINEER;')

test_query = '''SELECT 'Hello3';
SELECT * FROM "DAVIDE_MASINI"."SANDBOX"."ACCOUNTS" LIMIT 200;
SELECT 'Hello4';
  
SELECT * FROM "DAVIDE_MASINI"."SANDBOX"."ACCOUNTS" LIMIT 200;
     '''

for i in sql2xlsxlib.query2xlsx(sf_db, test_query, '/Users/Davide.Masini/test.xlsx'):
    pass

