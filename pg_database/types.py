import datetime
import json
import logging

from frozendict import frozendict
from geoalchemy2 import types as gistypes
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import sqltypes

from .conf import settings

logger = logging.getLogger(__name__)

COLUMN_TYPE_MAP = frozendict({
    "bool": sqltypes.Boolean,
    "boolean": sqltypes.Boolean,
    "bigint": sqltypes.BigInteger,
    "binary": sqltypes.LargeBinary,
    "bytea": sqltypes.LargeBinary,
    "geometry": gistypes.Geometry,
    "geography": gistypes.Geography,
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
    "raster": gistypes.Raster,
    "string": sqltypes.UnicodeText,
    "text": sqltypes.UnicodeText,
    "timestamp": sqltypes.DateTime,
    "unicode": sqltypes.UnicodeText,
    "varchar": sqltypes.UnicodeText,
})

SQL_TYPE_MAP = frozendict({
    "big_integer": "bigint",
    "binary": "bytea",
    "datetime": "timestamp",
    "double": "numeric",
    "large_binary": "bytea",
    "number": "numeric",
    "string": "text",
    "unicode": "text",
    "unicode_text": "text",
})

DATE_FORMAT = settings.date_format
DATETIME_FORMAT = settings.timestamp_format

DATE_FORMAT_MAP = frozendict({
    sqltypes.DateTime: "'{value}'::timestamp",
    sqltypes.Date: "'{value}'::date",
    datetime.datetime: DATETIME_FORMAT,
    datetime.date: DATE_FORMAT,
})


def column_type_for(column_type, default='unicode'):
    """
    Helper to map incoming column_type to a sqlalchemy.sql.sqltypes class.
    :param column_type: a string, sqltypes class or sqltypes instance to map
    :param default: the default column type to use if no mapping is possible
    """
    if isinstance(column_type, sqltypes.TypeEngine):
        return column_type
    elif isinstance(column_type, type) and issubclass(column_type, sqltypes.TypeEngine):
        return column_type
    else:
        return COLUMN_TYPE_MAP.get(column_type.lower(), COLUMN_TYPE_MAP[default])


def to_date_string(column_type, value):
    """
    Helper to convert a value of type column_type to a sqlalchemy.sql.sqltypes class.
    :param column_type: the date or datetime class, or a sqlalchemy.sql.sqltypes class
    :param value: a date or datetime instance, or a date formatted string value
    """
    str_format = DATE_FORMAT_MAP.get(type(value), DATETIME_FORMAT)
    sql_format = DATE_FORMAT_MAP[column_type]

    if isinstance(value, datetime.date):
        # It will be either a date or a datetime instance
        value = datetime.datetime.strftime(value, str_format)

    return sql_format.format(value=value)


def to_json_string(column_type, value):
    """
    Helper to convert a value of type column_type to a sqlalchemy.sql.sqltypes class.
    :param column_type: a class inheriting from sqlalchemy.sql.sqltypes.JSON
    :param value: a JSON compatible value or a string containing JSON content
    """
    if isinstance(value, dict):
        value = json.dumps(value)
    return f"'{value}'::{column_type.__name__}"


def type_to_string(column_type):
    """
    Helper to derive SQL string value from the provided type
    :param column_type: a string, or a sqlalchemy.sql.sqltypes class or instance
    """
    if hasattr(column_type, "name"):
        column_type = column_type.name
    if hasattr(column_type, "__visit_name__"):
        column_type = column_type.__visit_name__

    column_type = (column_type or "").lower()

    if column_type in SQL_TYPE_MAP:
        return SQL_TYPE_MAP[column_type]
    elif column_type in COLUMN_TYPE_MAP:
        return column_type

    logger.warning(f"type_to_string: unrecognized column type {column_type}")

    return column_type
