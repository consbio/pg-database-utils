import subprocess
import sys

from setuptools import Command, setup


class RunTests(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        errno = subprocess.call([sys.executable, '-m', 'unittest', 'pg_database.tests.tests'])
        raise SystemExit(errno)


with open('README.md') as readme:
    long_description = readme.read()


setup(
    name='pg_database_utils',
    description='A suite of utilities for PostgreSQL database queries and operations built on sqlalchemy',
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords='postgres,postgresql,utils,utilities,pg_database,sqlalchemy,sqlalchemy_utils',
    version='0.1',
    packages=[
        'pg_database', 'pg_database.tests'
    ],
    install_requires=[
        'sqlalchemy>=1.3.0"', 'sqlalchemy_utils>=0.36.0'
    ],
    url='https://github.com/consbio/pg-database-utils',
    license='BSD',
    cmdclass={'test': RunTests}
)
