import subprocess

from setuptools import Command, setup


class RunTests(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        errno = subprocess.call(["py.test", "pg_database/tests/tests.py", "--cov=pg_database", "--cov-branch"])
        raise SystemExit(errno)


with open("README.md") as readme:
    long_description = readme.read()

setup(
    url="https://github.com/consbio/pg-database-utils",
    name="pg-database-utils",
    description="A suite of utilities for PostgreSQL database queries and operations built on sqlalchemy",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="postgres,postgresql,utils,utilities,pg_database,pg_database_utils,sqlalchemy",
    version="1.0.0",
    license="BSD",
    packages=[
        "pg_database", "pg_database.tests"
    ],
    install_requires=[
        "frozendict>=2.0", "psycopg2-binary>=2.7.7", "sqlalchemy==1.3.*", "GeoAlchemy2"
    ],
    tests_require=["pytest", "pytest-cov"],
    cmdclass={"test": RunTests}
)
