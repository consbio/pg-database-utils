import datetime
import json

from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import sqltypes


COLUMN_TYPE_MAP = {
    "bool": sqltypes.Boolean,
    "boolean": sqltypes.Boolean,
    "bigint": sqltypes.BigInteger,
    "int": sqltypes.Integer,
    "integer": sqltypes.Integer,
    "float": sqltypes.Float,
    "date": sqltypes.Date,
    "datetime": sqltypes.DateTime,
    "decimal": sqltypes.Numeric,
    "double": sqltypes.Numeric,
    "numeric": sqltypes.Numeric,
    "number": sqltypes.Numeric,
    "json": postgresql.json.JSON,
    "jsonb": postgresql.json.JSONB,
    "unicode": sqltypes.UnicodeText,
}

DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

DATE_FORMAT_MAP = {
    sqltypes.DateTime: "'{value}'::timestamp",
    sqltypes.Date: "'{value}'::date",
    datetime.datetime: DATETIME_FORMAT,
    datetime.date: DATE_FORMAT,
}


def column_type_for(col_type, default='unicode'):
    if isinstance(col_type, sqltypes.TypeEngine):
        return col_type
    elif isinstance(col_type, type) and issubclass(col_type, sqltypes.TypeEngine):
        return col_type
    else:
        return COLUMN_TYPE_MAP.get(col_type.lower(), COLUMN_TYPE_MAP[default])


def to_date_string(col_type, value):
    str_format = DATE_FORMAT_MAP.get(type(value), DATETIME_FORMAT)
    sql_format = DATE_FORMAT_MAP[col_type]

    if isinstance(value, datetime.date):
        value = datetime.datetime.strftime(value, str_format)
    return sql_format.format(value=value)


def to_json_string(col_type, value):
    if isinstance(value, dict):
        value = json.dumps(value)
    return f"'{value}'::{col_type.__name__}"
