import os, sys
import xlsxlib

XLSX_FILEPATH = "c:/temp/t.xlsx"

sheet_name = "some data"
columns = [("a", None), ("b", None), ("c", None)]
rows = [(1, 2, 3), (4, 5, 6)]
sheets = [
    (sheet_name, columns, rows),
    (sheet_name, columns, rows),
]

for output in xlsxlib.xlsx(sheets, XLSX_FILEPATH):
    print(output)

os.startfile(XLSX_FILEPATH)