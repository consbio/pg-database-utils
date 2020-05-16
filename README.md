# pg-database-utils

[![Build Status](https://travis-ci.org/consbio/pg-database-utils.png?branch=master)](https://travis-ci.org/consbio/pg-database-utils)[![Coverage Status](https://coveralls.io/repos/github/consbio/pg-database-utils/badge.svg?branch=master)](https://coveralls.io/github/consbio/pg-database-utils?branch=master)

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
Install with `pip install pg_database_utils`.

## Configuration

This libary can be configured to work without Django or along-side Django.
Configuration involves two steps:
1. Create a JSON configuration file
2. Set the `DATABASE_CONFIG_JSON` environment variable to point to the location of the file

**To configure this project along-side Django**:

```python
{
    "django-db-key": "default"
}
```

> If "django-db-key" is set, it will take precedence over other database connection settings

**To configure this project by itself**:

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

**Other configuration options include**:

```python
{
    "date-format": "optional",      # Defaults to "%Y-%m-%d" for converting date strings
    "timestamp-format": "optional"  # Defaults to "%Y-%m-%d %H:%M:%S" for converting datetime strings
}
```

## Usage

One of the goals of this library is to make common database operations easy and readable.
Many of the utility functions therefore are designed to require as few imports from `sqlalchemy` as possible.

**Here are some of the available schema utilities**

* Creating and relating tables
```python
from pg_database import schema

schema.create_table(
    "my_table",
    dropfirst=True,
    index_cols={"id": "unique"},
    id="int", name="int", addr="text", deleted="bool"
)
schema.create_index("my_table", "name", index_op="unique")

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
```
* Dropping tables
```python
from pg_database import schema

all_tables = schema.get_metadata().tables
other_table = all_tables["other_table"]

schema.drop_foreign_key("other_table", "other_table_my_table_id_fkey")
schema.drop_index("my_table", index_name="my_table_json_col_json_full_idx")
schema.drop_table("my_table")
schema.drop_table("other_table")
```
* Inserting rows
```python
import json
from datetime import datetime, timedelta
from pg_database import sql, schema

create_date = datetime.now()

sql.select_into(
    "new_table",
    [(1, "one", {}, create_date), (2, "two", {}, create_date), (3, "three", {}, create_date)],
    "id,val,json,created",
    "int,text,jsonb,date"
)
```
* Updating rows
```python
from pg_database import sql

def update_row(row):
    row = list(row)
    pk = row[0]
    val = row[1]
    created = row[2]
    jval = row[3]
    row[1] = f"{pk} {val} first batch"
    row[2] = created + timedelta(days=1)
    row[3] = {"id": pk, "val": val, "batch": "first"}
    return row

sql.update_rows("new_table", "id", "val,created,json", update_row, batch_size=3)
```
* Querying rows
```python
from pg_database import sql, schema

schema.create_index("new_table", "json", index_op="json_path")
schema.create_index("new_table", "val", index_op="to_tsvector")

sql.query_json_keys("new_table", "json", {"batch": "first"})
sql.query_tsvector_columns("new_table", "val", "batch first")
```
