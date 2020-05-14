import re

SAFE_SQL_REGEX = re.compile(r"^[0-9A-Za-z_]{1,31}$")


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


def validate_sql_params(empty_message=None, **params):
    """
    Helper for validating parameters to be passed to sqlalchemy for sql injection
    :param empty_message: a custom error message for required params
    :param params: a dict of parameter names to form the error message, for example:
        table="invalid table"            --> "Invalid table name: invalid table"
        sql_types=["invalid!", "types!"] --> "Invalid sql types: invalid!, types!"
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
                invalid = ",".join(param_vals)
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
