# pg-database-utils

[![Build Status](https://travis-ci.org/consbio/pg-database-utils.png?branch=main)](https://travis-ci.org/consbio/pg-database-utils)
[![Coverage Status](https://coveralls.io/repos/github/consbio/pg-database-utils/badge.svg?branch=main)](https://coveralls.io/github/consbio/pg-database-utils?branch=main)

A suite of utilities for PostgreSQL database queries and operations built on sqlalchemy.

This library includes support for:
1. `TSVECTOR`, `JSON` and `JSONB` indexes (for PostgreSQL versions 9.5+)
2. Generated columns (for PostgreSQL versions 12+)
3. Optional Django database configuration for Django projects

It also includes:
1. Helpers to make most common DDL queries more readable
2. Performant functions for querying JSON and TSVECTOR columns
3. Support for `SELECT INTO` queries from existing tables and/or `VALUES` clauses
4. Support for `UPDATE` queries that require application logic


## Installation
Install with:
```bash
pip install pg-database-utils
```

## Configuration

This project is designed to make configuration easy.
If you already have database connections defined in Django, then you can reuse them;
otherwise, you can configure your own without having Django as a dependency.

### To configure with Django

If you want to use the "default" database, **no configuration is required**.

If you want to specify a particular Django database to read settings from:
1. Create a JSON configuration file with the database name:
```python
{
    "django-db-key": "other",               # To override DATABASES["default"]
    "connect-args": {"sslmode": "require"}  # To override DATABASES["<db_name>"]["OPTIONS"]
}
```
2. Set the `DATABASE_CONFIG_JSON` environment variable to point to the location of the file

**Note**: "django-db-key" takes precedence over all other database connection settings in the JSON file.
If you specify a Django database, those database connection settings will be used.

### To configure without Django

1. Create a JSON configuration file with at least the required settings (i.e. `database-name`):
```python
{
    "database-name": "required",     # Name of the database to query
    "database-engine": "optional",   # Defaults to postgres
    "database-host": "optional",     # Defaults to 127.0.0.1
    "database-port": "optional",     # Defaults to 5432
    "database-user": "optional",     # Defaults to postgres
    "database-password": "optional"  # For trusted users like postgres
}
```
2. Set the `DATABASE_CONFIG_JSON` environment variable to point to the location of the file

### Additional configuration with or without Django

Additional configuration options include:
```python
{
    "connect-args": {"sslmode": "require"},  # Defaults to postgres settings, "prefer" by default
    "date-format": "optional",               # Defaults to "%Y-%m-%d"
    "timestamp-format": "optional",          # Defaults to "%Y-%m-%d %H:%M:%S"
    "pooling-args": {                        # To override sqlalchemy pooling config
        "max_overflow": 0,                   # Defaults to 10 connections beyond pool size
        "pool_recycle": 60,                  # Defaults to no timeout (-1) in seconds
        "pool_size": 20,                     # Defaults to 5 connections
        "pool_timeout": 30                   # Defaults to 30 seconds
     }
}
```

**Note**: "date-format" and "timestamp-format" must be compatible with the formatting configured in PostgreSQL.


## Usage

This library is designed to make common database operations easy and readable,
so most of the utility functions are designed to work with either strings or `sqlalchemy` objects as parameters.

### Schema utilities

* Creating and relating tables
```python
from pg_database import schema

my_table = schema.create_table(
    "my_table",
    dropfirst=True,
    index_cols={"id": "unique"},
    id="int", name="int", addr="text", geom="bytea", deleted="bool"
)
schema.create_index(my_table, "name", index_op="unique")

schema.create_table("other_table", id="int", my_table_id="int", val="text")
schema.create_foreign_key("other_table", "my_table_id", "my_table.id")
```
* Altering tables
```python
from pg_database import schema

schema.alter_column_type("my_table", "name", "text")
schema.create_index("my_table", "name", index_op="to_tsvector")

schema.create_column("my_table", "json_col", "jsonb", checkfirst=True)
schema.create_index("my_table", "json_col", index_op="json_full")

# These steps require the postgis extension
schema.alter_column_type("my_table", "geom", "geometry", using="geom::geometry(Polygon,4326)")
schema.create_index("my_table", "geom", index_op="spatial")
```
* Dropping database objects
```python
from pg_database import schema

all_tables = schema.get_metadata().tables
other_table = all_tables["other_table"]

schema.drop_foreign_key(other_table, "other_table_my_table_id_fkey")
schema.drop_index("my_table", index_name="my_table_json_col_json_full_idx")
schema.drop_table("my_table")
schema.drop_table(other_table)
```

### SQL utilities

* Inserting rows
```python
import json
from datetime import datetime, timedelta
from pg_database import sql

create_date = datetime.now()

sql.select_into(
    "new_table",
    [
        (1, "one", {}, create_date),
        (2, "two", {}, create_date),
        (3, "three", {}, create_date)
    ],
    "id,val,json,created",
    "int,text,jsonb,date"
)
```
* Updating rows
```python
from pg_database import sql

def update_row(row):
    row = list(row)
    pk, val, created, jval = row[0], row[1], row[2], row[3]
    row[1] = f"{pk} {val} first batch"
    row[2] = created + timedelta(days=1)
    row[3] = {"id": pk, "val": val, "batch": "first"}
    return row

sql.update_rows("new_table", "id", "val,created,json", update_row, batch_size=3)
```
* Querying rows
```python
from pg_database import sql, schema

# Reduce database queries by sending a sqlalchemy table
all_tables = schema.get_metadata().tables
new_table = all_tables["new_table"]

schema.create_index(new_table, "json", index_op="json_path")
schema.create_index(new_table, "val", index_op="to_tsvector")

sql.query_json_keys(new_table, "json", {"batch": "first"})
sql.query_tsvector_columns("new_table", "val", "batch first")
```
* Values clause for `INSERT`s or `SELECT INTO`, with custom connection arguments at execution time
```python
from datetime import datetime
from sqlalchemy import column
from sqlalchemy.sql import Insert, Select
from pg_database import sql, schema

# Prepare data, column names, column types and table name

create_date = datetime.now()

values_data = [
    (1, "one", {}, True, create_date),
    (2, "two", {}, False, create_date),
    (3, "three", {}, 0, create_date)
]
values_names = ["id", "val", "json", "boolean", "created"]
values_types = ["int", "text", "jsonb", "bool", "date"]
values_table = "values_table"

# SELECT INTO to create a new table from raw values using sslmode==require

select_vals = sql.Values(values_names, values_types, *values_data)
select_into = sql.SelectInto([column(c) for c in values_names], values_table)
with schema.get_engine(connect_args={"sslmode": "require"}).connect() as conn:
    conn.execute(select_into.select_from(select_vals).execution_options(autocommit=True))

# INSERT INTO to add new records from raw values using custom pooling args

existing_table = schema.get_metadata().tables[values_table]

insert_vals = sql.Values(values_names, values_types, *values_data)
insert_from = Select([column(c) for c in values_names]).select_from(insert_vals)
insert_into = Insert(existing_table).from_select(names=values_names, select=insert_from)
with schema.get_engine(pooling_args={"pool_size": 20, "max_overflow": 0}).connect() as conn:
    conn.execute(insert_into.execution_options(autocommit=True))
```
