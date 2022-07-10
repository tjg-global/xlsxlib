import os, sys
import glob
import setuptools

setuptools.setup(
    name='xlsxlib',
    version='1.1.2',
    description='SQL to Excel',
    author='Tim Golden',
    author_email='tim.golden@global.com',
    modules = ["sql2xlsxlib.py", "xlsxlib.py"]
)
