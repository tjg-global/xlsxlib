import os, sys
import glob
import setuptools

setuptools.setup(
    name='xlsxlib',
    version='1.1.3',
    description='SQL to Excel',
    author='Tim Golden',
    author_email='tim.golden@global.com',
    install_requires = ['openpyxl', 'pyodbc', 'snowflake-connector-python'],
    #~ py_modules = ["sql2xlsxlib", "xlsxlib"],
    packages = ["xlsxlib"],
    entry_points = {
        "console_scripts" : [
            "gbundle=gbundle.gbundle:command_line",
            "xl=xlsxlib.xl:command_line",
        ]
    }
)
