import os, sys
import sql2xlsxlib
import pyodbc

table_name = input("Enter table: ") # merged.wmstype

query = """
SELECT 'name1';
SELECT * FROM david_table LIMIT 10;
SELECT 'name2';
SELECT * FROM david_table LIMIT 10;
"""

db = pyodbc.connect(driver="SQL Server", server="SVR-DWSQLDEV", database="STAGING")
for i in sql2xlsxlib.query2xlsx(db, query, "c:/temp/test.xlsx"):
    print(i)

os.startfile("%s.xlsx" % table_name)
