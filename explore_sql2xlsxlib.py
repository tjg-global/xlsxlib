import os, sys
import sql2xlsxlib
import pyodbc

table_name = input("Enter table: ")

db = pyodbc.connect(driver="SQL Server", server="SVR-DWSQLDEV", database="STAGING")
for i in sql2xlsxlib.table2xlsx(db, table_name):
    print(i)

os.startfile("%s.xlsx" % table_name)
