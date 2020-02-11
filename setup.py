import os, sys
import glob
import setuptools

setuptools.setup(
    name='python-site-packages',
    version='1.1',
    description='Exterion packages',
    author='Tim Golden',
    author_email='tim.golden@exterionmedia.co.uk',
    modules = [os.path.basename(f) for f in glob.glob("*.py")]
)
