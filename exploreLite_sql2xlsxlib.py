import sqlite3

import sql2xlsxlib

lite_db = sqlite3.connect('chinook.db')

print(lite_db.__class__.__module__)

test_query = '''SELECT 'Hello3'; 

SELECT * FROM "albums" LIMIT 100;
SELECT 'Hello4';


SELECT * FROM "employees" LIMIT 100;     '''

for i in sql2xlsxlib.query2xlsx(lite_db, test_query, 'test.xlsx'):
    pass