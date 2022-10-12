import os, sys
import time
import xlsxlib

XLSX_FILEPATH = "c:/temp/t.xlsx"

def genrows():
    yield 1, 2, 3
    time.sleep(0.5)
    yield 4, 5, 6
    time.sleep(0.5)
    yield 7, 8, 9

sheet_name = "some data"
columns = [("a", None), ("b", None), ("c", None)]
rows = genrows()
sheets = [
    (sheet_name, columns, rows),
    (sheet_name, columns, rows),
]

for output in xlsxlib.xlsx(sheets, XLSX_FILEPATH):
    print(output)

os.startfile(XLSX_FILEPATH)