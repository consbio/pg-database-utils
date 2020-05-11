import re

SAFE_SQL_REGEX = re.compile(r"^[0-9A-Za-z_]{1,31}$")


def validate_columns(table, column_names, message=None):
    """
    Helper for ensuring that all column names exist in a table
    :param table: a sqlalchemy table object
    :param column_names: a list of string column names
    :param message: a custom error message (precedes list of invalid columns)
    """

    table_cols = table.columns
    error_text = message or f'Invalid column names for "{table.name}"'

    if not column_names:
        raise ValueError(f"{error_text}: empty")
    if not all(c in table_cols for c in column_names):
        invalid_columns = ", ".join(set(column_names).difference(c.name for c in table_cols))
        raise ValueError(f"{error_text}: {invalid_columns}")
