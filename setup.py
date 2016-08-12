# setup.py for readthedocs. We need this to install into a venv so we
# can pull in dependencies.
from setuptools import setup

setup(name='interface-pgsql',
      version='1.0.0',
      author='Stuart Bishop',
      author_email='stuart.bishop@canonical.com',
      license='GPL3',
      py_modules=['requires'],
      install_requires=open('test_requirements.txt',
                            'r').read().splitlines())
