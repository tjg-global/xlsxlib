import os, sys
import glob
import setuptools

setuptools.setup(
    name='python-site-packages',
    version='1.1',
    description='SQL to Excel',
    author='Tim Golden',
    author_email='tim.golden@global.com',
    modules = [os.path.basename(f) for f in glob.glob("*.py")]
)
