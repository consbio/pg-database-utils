import datetime
import json

from frozendict import frozendict
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import sqltypes

from .conf import settings


COLUMN_TYPE_MAP = frozendict({
    "bool": sqltypes.Boolean,
    "boolean": sqltypes.Boolean,
    "bigint": sqltypes.BigInteger,
    "binary": sqltypes.LargeBinary,
    "bytea": sqltypes.LargeBinary,
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
})

DATE_FORMAT = settings.date_format
DATETIME_FORMAT = settings.timestamp_format

DATE_FORMAT_MAP = frozendict({
    sqltypes.DateTime: "'{value}'::timestamp",
    sqltypes.Date: "'{value}'::date",
    datetime.datetime: DATETIME_FORMAT,
    datetime.date: DATE_FORMAT,
})


def column_type_for(col_type, default='unicode'):
    """
    Helper to map incoming col_type to a sqlalchemy.sql.sqltypes class.
    :param col_type: a string, sqltypes class or sqltypes instance to map
    :param default: the default column type to use if no mapping is possible
    """
    if isinstance(col_type, sqltypes.TypeEngine):
        return col_type
    elif isinstance(col_type, type) and issubclass(col_type, sqltypes.TypeEngine):
        return col_type
    else:
        return COLUMN_TYPE_MAP.get(col_type.lower(), COLUMN_TYPE_MAP[default])


def to_date_string(col_type, value):
    """
    Helper to convert a value of type col_type to a sqlalchemy.sql.sqltypes class.
    :param col_type: the date or datetime class, or a sqlalchemy.sql.sqltypes class
    :param value: a date or datetime instance, or a date formatted string value
    """
    str_format = DATE_FORMAT_MAP.get(type(value), DATETIME_FORMAT)
    sql_format = DATE_FORMAT_MAP[col_type]

    if isinstance(value, datetime.date):
        # It will be either a date or a datetime instance
        value = datetime.datetime.strftime(value, str_format)

    return sql_format.format(value=value)


def to_json_string(col_type, value):
    """
    Helper to convert a value of type col_type to a sqlalchemy.sql.sqltypes class.
    :param col_type: a class inheriting from sqlalchemy.sql.sqltypes.JSON
    :param value: a JSON compatible value or a string containing JSON content
    """
    if isinstance(value, dict):
        value = json.dumps(value)
    return f"'{value}'::{col_type.__name__}"
