import os, sys
import glob
import setuptools

setuptools.setup(
    name='xlsxlib',
    version='1.19.00',
    description='SQL to Excel',
    author='Tim Golden',
    author_email='tim.golden@global.com',
    install_requires = ['openpyxl', 'pyodbc', 'snowflake-connector-python'],
    packages = ["xlsxlib"],
    entry_points = {
        "console_scripts" : [
            "xl=xlsxlib.xl:command_line",
            "runsql=xlsxlib.runsql:command_line",
            "xlload=xlsxlib.xlload:command_line",
            "dump_snowflake_databases=xlsxlib.dump_snowflake_databases:command_line",
        ]
    }
)
