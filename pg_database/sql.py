import json
import logging

from geoalchemy2.types import Geometry
from sqlalchemy import column, exc, table, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import sqltypes, and_, or_, func, FromClause, Insert, Select, Update

from .schema import get_engine, get_table, get_tables, get_table_count, table_exists, drop_table
from .types import column_type_for, to_date_string, to_json_string
from .validation import validate_columns_in, validate_sql_params, validate_table_name

logger = logging.getLogger(__name__)


def _query_limited(table, query, limit=None):
    """ Helper to limit query results without the full table scan executed by LIMIT """

    with table.bind.connect() as conn:
        if limit is None:
            return list(conn.execute(query).fetchall())

        cursor = conn.execute(query)
        results = []

        try:
            for _ in range(limit):
                results.append(next(cursor))
        except StopIteration:
            pass

        return results


def query_json_keys(table_or_name, column_name, query, limit=None):
    """
    Queries a JSON column matching keys: performs best with a `jsonb_path_ops` index:
        create_index("table1", "json_col", index_op="json_path")

    Example SQL generated:
        SELECT * FROM table1 WHERE table1.json_col @> '{"key1": "val1", ...}'

    :param table_or_name: a sqlalchemy table object or the name of a table to query
    :param column_name: the name of the (indexed) column with keys to query
    :param query: a dict or JSON string containing keys and values to match
    :param limit: an optional number of rows to limit the query results
    """

    if isinstance(table_or_name, str):
        table = get_table(table_or_name)
    else:
        table = table_or_name

    validate_columns_in(table, [column_name], empty_table=table_or_name)

    json_op = table.columns[column_name].op("@>")

    if isinstance(query, dict):
        json_query = Select(table.columns).where(json_op(json.dumps(query)))
    else:
        json_query = Select(table.columns).where(json_op(query))

    return _query_limited(table, json_query, limit)


def query_tsvector_columns(table_or_name, column_names, query, limit=None):
    """
    Performs a full text search on one or more columns: performs best if all column names are indexed
        create_index("table1", "col1,col2", index_op="to_tsvector")

    Example SQL generated:
        SELECT DISTINCT * FROM table1
        WHERE to_tsvector(col1||' '||col2||' '||...) @@ to_tsquery('english', '''query''')

    :param table_or_name: a sqlalchemy table object or the name of a table to query
    :param column_names: one or more comma-seperated tsvector-indexed column names, or a list of them
    :param query: a query to be parsed for matching a tsvector index
    :param limit: an optional number of rows to limit the query results
    """

    if isinstance(table_or_name, str):
        table = get_table(table_or_name)
    else:
        table = table_or_name

    if isinstance(column_names, str):
        column_names = [c.strip() for c in column_names.split(",")]

    validate_columns_in(table, column_names, empty_table=table_or_name)

    search_condition = func.to_tsvector(text("||' '||".join(column_names), postgresql.TSVECTOR)).match(
        # Triple quotes required since `plainto_tsquery` cannot be directly invoked via sqlalchemy
        # Calling `to_tsvector.match` generates the following, which results in a SQL syntax error
        #    WHERE to_tsvector('english', <columns>) @@ to_tsquery('english', <query>)
        # What we need is to use `plainto_tsquery`, or otherwise, make sure query is a valid tsquery string:
        #    WHERE to_tsvector('english', <columns>) @@ plainto_tsquery('english', <query>)
        # See: https://github.com/sqlalchemy/sqlalchemy/issues/3160 (for info on this issue in sqlalchemy)
        f"'''{query}'''",
        postgresql_regconfig="english",
    )

    return _query_limited(table, Select(table.columns).distinct().where(search_condition), limit)


def update_from(table_name, into_table_name, join_columns, target_columns=None, engine=None):
    """
    Updates records in one table from values in another

    :param table_name: the name of the table from which to query updated values
    :param into_table_name: the name of the table to be updated
    :param join_columns: one or more column names that constitute unique records for joining
    :param target_columns: an optional reduced list of column names to target for updates
    :param engine: an optional sqlalchemy.engine to use in the UPDATE query
    """

    both_tables = get_tables(engine=engine)
    from_table = both_tables.get(table_name)
    into_table = both_tables.get(into_table_name)

    if isinstance(join_columns, str):
        join_columns = [c.strip() for c in join_columns.split(",")]

    validate_columns_in(
        from_table, join_columns,
        empty_table=table_name,
        message=f"Join columns missing in source table {table_name}"
    )
    validate_columns_in(
        into_table, join_columns,
        empty_table=into_table_name,
        message=f"Join columns missing in target table {into_table_name}"
    )

    # Prepare column names with values to be updated

    log_message = f"update_from: updating {into_table_name}"

    from_cols = from_table.columns
    into_cols = into_table.columns

    if isinstance(target_columns, str):
        target_columns = target_columns.split(",")

    if target_columns is None or "*" in target_columns:
        log_message += f" from all columns in {table_name}"
        update_cols = {c.name: c for c in from_cols}
    else:
        log_message += f" from specified columns in {table_name}"
        update_cols = {c.name: c for c in from_cols if c.name in target_columns}

    update_cols = {k: v for k, v in update_cols.items() if k not in join_columns}

    if not update_cols:
        logger.warning("update_from: no non-primary key columns to update")
        return
    elif target_columns and len(target_columns) > len(update_cols):
        ignore_cols = ", ".join(c for c in target_columns if c not in update_cols)
        logger.warning(f"update_from: ignoring columns: {ignore_cols}")

    # Prepare query with specified columns and filtering

    join_where = and_(*[from_cols[col] == into_cols[col] for col in join_columns])
    skip_where = or_(
        *[
            # Use ST_Equals for geometry column (NullType) comparison
            ~func.ST_Equals(from_cols[col], into_cols[col])
            if isinstance(from_cols[col].type, Geometry)
            else from_cols[col].op("IS DISTINCT FROM")(into_cols[col])
            for col in update_cols
        ]
    )
    update_from = Update(into_table, whereclause=join_where).where(skip_where).values(update_cols)

    logger.info(log_message + f", joining on: {join_columns}")

    with from_table.bind.connect() as conn:
        conn.execute(update_from.execution_options(autocommit=True))


def update_rows(table_name, join_columns, target_columns, update_row, batch_size=-1, engine=None):
    """
    Updates records in the given table with custom modified values.

    :param table_name: the name of the table with row values to be modified
    :param join_columns: one or more column names that constitute unique records
    :param target_columns: the list of column names to target for updates
    :param update_row: a callable for updating each row:
        The provided function must take a single argument, which will be the row to update.
        It must either return a list of exactly the same length with updated values, or None.
        If None, that row will be left unmodified in the original table.
        Incoming rows are sqlalchemy records: they are read-only unless converted to lists.
    :param batch_size: an optional number of rows to execute per batch; all rows by default
    :param engine: an optional sqlalchemy.engine to use in the UPDATE query
    """

    table = get_table(table_name, engine)

    if isinstance(join_columns, str):
        join_columns = [c.strip() for c in join_columns.split(",")]
    if isinstance(target_columns, str):
        target_columns = target_columns.split(",")

    validate_columns_in(
        table, join_columns,
        empty_table=table_name,
        message=f"Join columns missing in {table_name}"
    )
    validate_columns_in(
        table, target_columns,
        empty_table=table_name,
        message=f"Target columns missing in {table_name}"
    )

    if not callable(update_row) or isinstance(update_row, type):
        invalid = getattr(update_row, "__name__", "None")
        raise ValueError(f"Invalid update function: {invalid}")
    if not batch_size:
        raise ValueError(f"Invalid batch size: {batch_size}")

    # Prepare query with specified columns and filtering

    column_names = tuple(join_columns) + tuple(target_columns)

    # Ensure columns are ordered as provided
    col_types = [str(table.columns[c].type) for c in column_names]
    data_cols = [table.columns[c] for c in column_names]

    table_count = get_table_count(table)
    batch_size = table_count if batch_size < 0 else batch_size

    if table_count == 0:
        logger.info(f"update_rows:\tno rows to update")
        return

    tmp_table_name = f"tmp_{table_name}"

    try:
        with table.bind.connect() as conn:
            logger.info(f"update_rows: updating {table_name} with modified values in batches of {batch_size}\n")

            select_query = conn.execute(Select(data_cols).execution_options(stream_results=True))
            table_created = False

            for offset in range(0, table_count, batch_size):
                done_count = min(table_count, (offset + batch_size))
                next_count = min(batch_size, (table_count - done_count))

                logger.info(f"update_rows:\tpreparing the next {next_count} rows for update")

                updated_rows = (update_row(r) for r in select_query.fetchmany(batch_size))
                rows_to_send = [row for row in updated_rows if row is not None]

                if not rows_to_send:
                    continue
                elif table_created:
                    insert_into(tmp_table_name, rows_to_send, column_names, inspect=False, engine=engine)
                else:
                    select_into(tmp_table_name, rows_to_send, column_names, col_types, inspect=False, engine=engine)
                    table_created = True

                logger.info(f"update_rows:\tprocessed {done_count} of {table_count} rows\n")

        update_count = get_table_count(tmp_table_name) if table_created else 0

        if not update_count:
            logger.info(f"update_rows:\tno rows to update")
        else:
            if update_count == table_count:
                logger.info(f"update_rows:\tapplying changes to all {table_count} rows")
            else:
                logger.info(f"update_rows:\tapplying changes to only {update_count} of {table_count} rows")

            update_from(tmp_table_name, table_name, join_columns, target_columns, engine=engine)
    finally:
        drop_table(tmp_table_name)


def insert_from(
    table_name, into_table_name, column_names=None, join_columns=None, create_if_not_exists=False, engine=None
):
    """
    Inserts records from one table into another

    :param table_name: the name of the table from which to insert records
    :param into_table_name: the name of the table into which the records will go
    :param column_names: an optional reduced list of column names to specify for insertion
    :param join_columns: one or more column names that constitute unique records, not to be inserted
    :param create_if_not_exists: if True, create into_table_name if it doesn't exist, otherwise exit with warning
    :param engine: an optional sqlalchemy.engine to use in the UPDATE query
    """

    both_tables = get_tables(engine=engine)
    from_table = both_tables.get(table_name)
    into_table = both_tables.get(into_table_name)

    validate_table_name(from_table, table_name)

    if not table_exists(into_table):
        if not create_if_not_exists:
            raise ValueError(f"No table named {into_table_name} to insert into")
        return select_from(table_name, into_table_name, column_names, engine=engine)

    # Validate parameters for excluding unique records

    if isinstance(join_columns, str):
        join_columns = [c.strip() for c in join_columns.split(",")]

    if join_columns:
        validate_columns_in(
            from_table, join_columns,
            empty_table=table_name,
            message=f"Join columns missing in source table {table_name}"
        )
        validate_columns_in(
            into_table, join_columns,
            empty_table=into_table_name,
            message=f"Join columns missing in target table {into_table_name}"
        )

    # Prepare column names to be inserted

    log_message = f"insert_from: populating {into_table_name} from {table_name}"

    from_cols = from_table.columns
    into_cols = into_table.columns

    if isinstance(column_names, str):
        column_names = column_names.split(",")

    if column_names is None or "*" in column_names:
        log_message += f", with all columns in {table_name}"
        insert_cols = from_cols
    else:
        log_message += f", with specified columns in {table_name}"
        insert_cols = [c for c in from_cols if c.name in column_names]

    if not insert_cols:
        logger.warning("insert_from: no columns to insert")
        return
    elif column_names and len(column_names) > len(insert_cols):
        target_cols = set(c.name for c in insert_cols)
        ignore_cols = ", ".join(set(column_names).difference(target_cols))
        logger.warning(f"insert_from: ignoring columns: {ignore_cols}")

    # Prepare query with specified columns and filtering

    if not join_columns:
        insert_vals = Select(insert_cols).select_from(from_table)
    else:
        log_message += f", excluding those matching: {join_columns}"

        # Exclude records matching specified columns via outer join
        insert_from = from_table.outerjoin(
            into_table, and_(*[from_cols[col] == into_cols[col] for col in join_columns])
        )
        insert_vals = (
            Select(insert_cols)
            .select_from(insert_from)
            .where(and_(*[into_cols[col].is_(None) for col in join_columns]))
        )

    logger.info(log_message)

    insert_from = Insert(into_table).from_select(names=[c.name for c in insert_cols], select=insert_vals)
    with from_table.bind.connect() as conn:
        conn.execute(insert_from.execution_options(autocommit=True))


def insert_into(table_name, values, column_names, create_if_not_exists=False, inspect=True, engine=None):
    """
    Inserts a list of values into an existing table

    :param table_name: the name of the table into which to insert records
    :param values: a list of lists containing literal values to insert into the table
    :param column_names: one or more comma-seperated column names, or a list of column names
        * column names must correspond exactly to the order of the values provided
        example names:  'col1,col2,col3' OR ['col1', 'col2', 'col3']
        example values: [(0, 42, 'first'), (True, 86, 'next'), (1, -4, 'last')]
    :param create_if_not_exists: if True, create table_name if it doesn't exist, otherwise exit with warning
    :param inspect: if True, ensure all value rows correspond to the number of column names
    :param engine: an optional sqlalchemy.engine to use in the INSERT query
    """

    into_table = get_table(table_name, engine, ignore_missing=create_if_not_exists)

    if not table_exists(into_table):
        return select_into(table_name, values, column_names, inspect=inspect, engine=engine)

    if isinstance(column_names, str):
        column_names = column_names.split(",")

    validate_columns_in(into_table, column_names, empty_table=table_name)

    val_length = len(values)
    if not val_length:
        logger.warning(f"insert_into: no values to insert")
        return

    row_length = len(column_names)
    if inspect and not all(row_length == len(val) for val in values):
        raise ValueError(f"Values provided do not match columns: {column_names}")

    logger.info(f"insert_into: populating {table_name} from {val_length} value records")

    # Ensure column types are defined in the order column_names was given
    column_types = [str(into_table.columns[c].type) for c in column_names]

    insert_cols = [column(c) for c in column_names]
    insert_vals = Select(insert_cols).select_from(Values(column_names, column_types, *values))
    insert_into = Insert(into_table).from_select(names=column_names, select=insert_vals)

    with into_table.bind.connect() as conn:
        conn.execute(insert_into.execution_options(autocommit=True))


def select_from(table_name, into_table_name, column_names=None, engine=None):
    """
    Inserts records from a table into a new table via SELECT INTO

    :param table_name: the name of the table from which to insert records
    :param into_table_name: the name of the new table into which the records will go
    :param column_names: an optional reduced list of column names to specify for insertion
    :param engine: an optional sqlalchemy.engine to use in the SELECT INTO query
    """

    from_table = get_table(table_name, engine)

    validate_table_name(from_table, table_name)
    if table_exists(into_table_name):
        raise ValueError(f"Table {into_table_name} already exists")

    log_message = f"select_from: creating {into_table_name}"

    if isinstance(column_names, str):
        column_names = column_names.split(",")

    if column_names is None or "*" in column_names:
        log_message += f" from all columns in {table_name}"
        select_cols = from_table.columns
    else:
        log_message += f" from specified columns in {table_name}"
        select_cols = [c for c in from_table.columns if c.name in column_names]

    if not select_cols:
        logger.warning("select_from: no columns to insert")
        return
    elif column_names and len(column_names) > len(select_cols):
        target_cols = set(c.name for c in select_cols)
        ignore_cols = ", ".join(set(column_names).difference(target_cols))
        logger.warning(f"select_from: ignoring columns: {ignore_cols}")

    logger.info(log_message)

    select_from = SelectInto(select_cols, into_table_name).select_from(from_table)
    with from_table.bind.connect() as conn:
        conn.execute(select_from.execution_options(autocommit=True))


def select_into(table_name, values, column_names, column_types=None, inspect=True, engine=None):
    """
    Inserts a list of values into a new table via SELECT INTO

    :param table_name: the name of the table to create from the values provided
    :param values: a list of lists containing literal values to insert into the table
    :param column_names: one or more comma-seperated column names, or a list of column names
        * column names must correspond exactly to the order of the types and values provided
    :param column_types: an optional list of types corresponding to column names
        * column types must correspond exactly to the order of the columns and values provided
        * inserted columns will default to unicode text if types are invalid or not provided
        * see types.COLUMN_TYPE_MAP for string values that map to types

        example names:  'col1,col2,col3' OR ['col1', 'col2', 'col3']
        example types:  'bool,int,varchar' OR ['bool', 'int', 'varchar']
        example values: [(0, 42, 'first'), (True, 86, 'next'), (1, -4, 'last')]
    :param inspect: if True, ensure all value rows correspond to the number of column names
    :param engine: an optional sqlalchemy.engine to use in the SELECT INTO query
    """

    if table_exists(table_name):
        raise ValueError(f"Table {table_name} already exists")

    val_length = len(values)
    if not val_length:
        logger.warning(f"select_into: no values to insert")
        return

    if isinstance(column_names, str):
        column_names = column_names.split(",")
    if column_types and isinstance(column_types, str):
        column_types = column_types.split(",")

    validate_sql_params(column_names=column_names, empty_message="No columns to select")

    row_length = len(column_names)
    if column_types and row_length != len(column_types):
        raise ValueError(f"Column types provided do not match columns: {column_types}")
    elif inspect and not all(row_length == len(val) for val in values):
        raise ValueError(f"Values provided do not match columns: {column_names}")

    logger.info(f"select_into: creating {table_name} from {val_length} value records")

    select_cols = [column(c) for c in column_names]
    select_from = Values(column_names, column_types, *values)
    select_into = SelectInto(select_cols, table_name).select_from(select_from)

    with (get_engine() if engine is None else engine).connect() as conn:
        conn.execute(select_into.execution_options(autocommit=True))


class SelectInto(Select):
    """
    Implements SELECT INTO commands for PostgreSQL
    :see: https://groups.google.com/forum/#!msg/sqlalchemy/O4M6srJYzk0/B8Umq9y08EoJ
    """

    def __init__(self, columns, into, *arg, **kw):
        super(SelectInto, self).__init__(columns, *arg, **kw)
        self.into = into


@compiles(SelectInto)
def _select_into(element, compiler, **kw):
    text = compiler.visit_select(element, **kw)
    text = text.replace("FROM", f"INTO {element.into} \nFROM")
    return text


class Values(FromClause):
    """
    Implements the VALUES clause for SELECT INTO operations
    :see: https://stackoverflow.com/questions/18858291/values-clause-in-sqlalchemy
    """

    def __init__(self, cols, types=None, *args):

        if isinstance(cols, str):
            cols = [c.strip().join('""') for c in cols.split(",")]
        if isinstance(types, str):
            types = [c.strip() for c in types.split(",")]

        if not types:
            types = [sqltypes.UnicodeText for _ in range(len(cols))]
        elif len(cols) == len(types):
            types = [column_type_for(t) for t in types]
        else:
            raise exc.ArgumentError(f"Types do not correspond to columns:\n\ttypes: {types}\n\tcols:{cols}")

        self.cols = cols
        self.vals = args
        self.types = types


@compiles(Values)
def _values(element, compiler, **kwargs):
    """
    Compiles the VALUES clause for SELECT INTO operations
    :see: https://www.postgresql.org/docs/current/queries-values.html
    """

    value_cols = ",".join('"{}"'.format(c.strip("'\"")) for c in element.cols)
    value_sets = ", ".join(
        "({values})".format(
            values=",".join(_compile_value(compiler, val, element.types[idx]) for idx, val in enumerate(tup))
        )
        for tup in element.vals
    )
    return f'(VALUES {value_sets}) AS "values" ({value_cols})'


def _compile_value(compiler, value, type_):

    if value is None:
        return "NULL"
    elif issubclass(type_, (sqltypes.Date, sqltypes.DateTime)):
        return to_date_string(type_, value)
    elif issubclass(type_, sqltypes.JSON):
        return to_json_string(type_, value)

    if issubclass(type_, sqltypes.String):
        value = str(value)
    elif issubclass(type_, sqltypes.Numeric):
        value = float(value)

    return compiler.render_literal_value(value, type_())
