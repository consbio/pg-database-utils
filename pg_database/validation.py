import re

from sqlalchemy.sql import sqltypes

from pg_database import types


SAFE_SQL_REGEX = re.compile(r"^[0-9A-Za-z_]{1,63}$")
SQL_TYPE_REGEX = re.compile(r"^[0-9A-Za-z_\(\),]+$")
SQL_USING_REGEX = re.compile(r"^[0-9A-Za-z_\(\),:']+$")


def validate_columns_in(table, column_names, empty_table, message=None):
    """
    Helper for ensuring that all column names exist in a table
    :param table: a sqlalchemy table object
    :param column_names: a list of string column names
    :param empty_table: the expected name of the table if table is None
    :param message: a custom error message (precedes list of invalid columns)
    """

    validate_table_name(table, empty_table)

    table_cols = table.columns
    error_text = message or f'Invalid column names for "{table.name}"'

    if not column_names:
        raise ValueError(f"{error_text}: empty")
    if not all(c in table_cols for c in column_names):
        invalid_columns = ", ".join(set(column_names).difference(c.name for c in table_cols))
        raise ValueError(f"{error_text}: {invalid_columns}")


def validate_column_type(column_type):
    """
    Helper for validating a column type
    :param column_type: the type to validate, which may be:
        * a string indicating the type
        * a sqlalchemy.sql.sqltypes.TypeEngine class or instance
    :see types.COLUMN_TYPE_MAP: for string values that map to types
    """

    if not column_type:
        raise ValueError(f"Invalid column type: empty")
    elif isinstance(column_type, str):
        base_type = column_type.split("(", 1)[0]
        remaining = column_type[len(base_type):]

        if base_type.lower() not in types.COLUMN_TYPE_MAP:
            raise ValueError(f"Invalid column type: {column_type}")
        if remaining and not SQL_TYPE_REGEX.match(remaining):
            raise ValueError(f"Invalid column type: {column_type}")
    else:
        is_type_class = isinstance(column_type, sqltypes.TypeEngine)
        is_type_instance = isinstance(column_type, type) and issubclass(column_type, sqltypes.TypeEngine)

        if not is_type_class and not is_type_instance:
            raise ValueError(f"Invalid column type: {column_type}")

    return column_type


def validate_pooling_params(pooling_params):
    """ Helper to supported sqlalchemy pooling options """

    supported = ("max_overflow", "pool_recycle", "pool_size", "pool_timeout")

    if not pooling_params:
        return {}
    if not isinstance(pooling_params, dict):
        raise ValueError(f"Invalid pooling params: {pooling_params}")
    if any(param not in supported for param in pooling_params):
        invalid_params = ", ".join(set(pooling_params).difference(supported))
        raise ValueError(f"Invalid pooling params: {invalid_params}")
    if any(not isinstance(pooling_params[param], int) for param in pooling_params):
        invalid_params = ", ".join(param for param in pooling_params if not isinstance(pooling_params[param], int))
        raise ValueError(f"Pooling params require integer values: {invalid_params}")

    return pooling_params


def validate_sql_params(empty_message=None, **params):
    """
    Helper for validating parameters to be passed to sqlalchemy for sql injection
    :param empty_message: a custom error message for required params
    :param params: a dict of parameter names to form the error message, for example:
        table="bad-table"                    --> "Invalid table name: bad-table"
        sql_types=["bad-type", "wrong-type"] --> "Invalid sql types: bad-type, wrong-type"
    """

    for param, param_val in params.items():

        param_text = f"{param} name" if "_" not in param else " ".join(param.split("_"))
        param_vals = param_val if isinstance(param_val, (list, tuple)) else [param_val]

        if not param_vals and empty_message:
            raise ValueError(empty_message)

        for val in param_vals:
            if not val and empty_message:
                raise ValueError(empty_message)
            if not SAFE_SQL_REGEX.match(str(val or "")):
                invalid = ",".join(str(p or "") for p in param_vals)
                raise ValueError(f"Invalid {param_text}: {invalid}")


def validate_table_name(table, empty_table=None):
    """
    Helper for validating a table object
    :param table: a sqlalchemy table object
    :param empty_table: the expected name of the table if table is None
    """

    if not getattr(table, "exists", False):
        empty_table = getattr(table, "name", empty_table)
        raise ValueError(f"No table named {empty_table}")

    return table
