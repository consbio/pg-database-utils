import logging

from sqlalchemy import column, create_engine, exc, literal_column, select, table, text, Column, Table
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import AddConstraint, DropConstraint, ForeignKeyConstraint, Index, MetaData
from sqlalchemy.sql import and_, func, Select

from .conf import settings
from .types import column_type_for
from .validation import validate_columns_in, validate_sql_params

logger = logging.getLogger(__name__)


def get_engine():
    return create_engine(URL(**settings.database_info))


def get_metadata():
    metadata = MetaData(get_engine())
    metadata.reflect()
    return metadata


# Table utilities


def get_table(name):
    """ Auto-load a table schema from the database. Like `get_tables()`, but more efficient for a single table """

    engine = get_engine()

    try:
        return Table(name, MetaData(engine), autoload=True, autoload_with=engine)
    except exc.NoSuchTableError:
        raise ValueError(f'No table named "{name}"')


def get_table_count(table_or_name):
    if isinstance(table_or_name, str):
        table = get_table(table_or_name)
    else:
        table = table_or_name

    return select([func.count()]).select_from(table).scalar()


def get_tables(table_names=None):
    """
    A helper to pull in one or more sqlalchemy table objects from the database.
    :param table_names: one or more comma-seperated table names, or a list of table names
    :return: all of the CEQA tables in a dict-like object, or the specified subset in a dict
    """

    metadata = MetaData(get_engine())
    metadata.reflect()

    all_tables = metadata.tables

    if table_names is None:
        return all_tables
    elif isinstance(table_names, str):
        table_names = [t.strip() for t in table_names.split(",")]

    return {t: all_tables[t] for t in table_names if t in all_tables}


def table_exists(table_or_name):
    if isinstance(table_or_name, str):
        table = get_tables().get(table_or_name)
    else:
        table = table_or_name

    return table is not None and table.exists


def create_table(table_name, index_cols=None, drop_first=True, **column_types):

    validate_sql_params(table=table_name, empty_message=f"No table name specified")
    validate_sql_params(
        column_names=[cname for cname in column_types.keys()],
        empty_message=f"No column names specified for {table_name}"
    )

    meta = get_metadata()
    exists = table_name in meta.tables

    if exists and not drop_first:
        raise ValueError(f"Table already exists: {table_name}")
    elif exists and drop_first:
        meta.tables[table_name].drop(checkfirst=True)
        meta = get_metadata()

    cols = [Column(c, column_type_for(t)) for c, t in column_types.items()]
    table = Table(table_name, meta, *cols)
    table.create()

    if index_cols:
        if isinstance(index_cols, str):
            # Basic index: single or multi-column (if comma-separated)
            create_index(table, index_cols)
        else:
            # Specific index:
            #    Single or multi-column (if any item is comma-separated)
            #    Basic index type if not a dict; otherwise use type specified by dict values
            if not isinstance(index_cols, dict):
                index_cols = {}.fromkeys(index_cols)
            for col_names, index_op in index_cols.items():
                create_index(table, col_names, index_op=index_op)

    return table


def drop_table(table_or_name):
    if isinstance(table_or_name, str):
        table = get_tables().get(table_or_name)
    else:
        table = table_or_name

    if table is None:
        logger.warning(f"drop_table: no table found named {table_or_name}")
    else:
        logger.info(f"drop_table: dropping table named {table.name}")
        table.drop(checkfirst=True)


# Column utilities


def alter_column_type(table_or_name, column_name, new_type):

    if isinstance(table_or_name, str):
        table_name = table_or_name
    else:
        table_name = table_or_name.name

    validate_sql_params(table=table_name, column=column_name, column_type=new_type)

    if new_type != "bool":
        using = f"{column_name}::{new_type}"
    else:
        using = f"CASE WHEN {column_name}::int=0 THEN FALSE WHEN {column_name} IS NULL THEN NULL ELSE TRUE END"

    alter_sql = f"ALTER TABLE {table_name} "
    alter_sql += f"ALTER COLUMN {column_name} TYPE {new_type} USING {using}"

    logger.info(f"alter_column_type: altering {table_name}.{column_name} to {new_type}")

    with get_engine().connect() as conn:
        conn.execute(text(alter_sql).execution_options(autocommit=True))


def create_column(table_or_name, column_name, column_type, checkfirst=False, default=None, nullable=False):

    if isinstance(table_or_name, str):
        table_name = table_or_name
    else:
        table_name = table_or_name.name

    validate_sql_params(table=table_name, column=column_name, column_type=column_type)

    str_default = isinstance(default, str)
    if str_default:
        validate_sql_params(default_value=default)

    alter_table = f"ALTER TABLE {table_name} "

    if checkfirst:
        add_column = f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
    else:
        add_column = f"ADD COLUMN {column_name} {column_type}"

    constraints = ["NULL" if nullable else "NOT NULL"]

    if str_default:
        constraints.append(f"DEFAULT '{default}'")
    elif default is not None:
        constraints.append(f"DEFAULT {default}")

    create_sql = "{alter_table} {add_column} {constraints}".format(
        alter_table=alter_table, add_column=add_column, constraints=" ".join(constraints)
    )
    logger.info(f"create_column: creating {table_name}.{column_name} as {column_type}")

    with get_engine().connect() as conn:
        conn.execute(text(create_sql).execution_options(autocommit=True))


def create_tsvector_column(table_or_name, column_name, column_names, index_name):
    """
    Creates a single text tsvector column from a list of columns
    NOTE: compatible with PostgreSQL version 12.* and above
    TODO: update with sqlalchemy implementation when available
    :see: https://github.com/sqlalchemy/sqlalchemy/pull/4896
    """

    if isinstance(table_or_name, str):
        table_name = table_or_name
    else:
        table_name = table_or_name.name

    validate_sql_params(table=table_name, column=column_name, column_names=column_names)

    if not isinstance(column_names, str):
        column_names = ", ".join(column_names)

    generate_sql = "\n".join(
        (
            f"ALTER TABLE {table_name}",
            f"  ADD COLUMN {column_name} tsvector",
            "    GENERATED ALWAYS AS (",
            f"      TO_TSVECTOR('english', CONCAT({column_names}))",
            "    ) STORED;",
        )
    )
    logger.info(f"create_tsvector_column: executing generate column query:\n{generate_sql}")

    with get_engine().connect() as conn:
        conn.execute(generate_sql)

    create_index(table_or_name, column_names, index_name, "to_tsvector")


def drop_column(table_or_name, column_name):

    if isinstance(table_or_name, str):
        table_name = table_or_name
    else:
        table_name = table_or_name.name

    validate_sql_params(table=table_name, column=column_name)

    drop_sql = f"ALTER TABLE {table_name} DROP COLUMN {column_name}"

    logger.info(f"drop_column: dropping {table_name}.{column_name}")

    with get_engine().connect() as conn:
        conn.execute(text(drop_sql).execution_options(autocommit=True))


# Index utilities


def create_index(table_or_name, column_names, index_name=None, index_op=None):
    """
    Creates a database index on the specified table for one or more column names

    :param table_or_name: a sqlalchemy table object or the name of a table
    :param column_names: one or more comma-seperated column names, or a list of column names
    :param index_name: optionally override the default index name generated from column_names
        It may be necessary to override the index name to ensure it is under 63 characters;
        otherwise, index creation will fail with an error from PostgreSQL.
        A conventional index name will follow this pattern: "{table_name}_{column_names}_idx"
    :param index_op: any SQL function available in Postgresql, that is defined in sqlalchemy.sql.func.*
        Only "coalesce" and "to_tsvector" are directly supported.
        Otherwise, we assume the specified function takes the provided column names as parameters.
    """

    if isinstance(table_or_name, str):
        table = get_tables().get(table_or_name)
    else:
        table = table_or_name

    if isinstance(column_names, str):
        column_names = [c.strip() for c in column_names.split(",")]

    validate_columns_in(table, column_names, empty_table=table_or_name, message="Invalid index column names")
    if len(column_names) > 1 and (index_op or "").startswith("json"):
        raise ValueError(f"Invalid index operation for multiple columns: {index_op}")

    if not index_name:
        column_index = "_".join(col for col in column_names)
        index_name = f"{table.name}_{column_index}_idx"

    index_kwargs = {"_table": table}

    if not index_op:
        expressions = [col for col in column_names if col in table.columns]
    elif index_op == "coalesce":
        # Coalesce takes an indefinite list of column names and a default value
        expressions = (func.coalesce(*(column_names + [""])),)
    elif index_op == "json_full":
        # Generates CREATE INDEX <idx> ON <table> USING GIN (<col>);
        index_kwargs["postgresql_using"] = "GIN"
        expressions = (table.columns.get(column_names[0]),)
    elif index_op == "json_path":
        # Generates CREATE INDEX <idx> ON <table> USING GIN (<col> jsonb_path_ops);
        index_kwargs["postgresql_using"] = "GIN"
        expressions = (text("{col} jsonb_path_ops".format(col=column_names[0])),)
    elif index_op == "to_tsvector":
        index_kwargs["postgresql_using"] = "GIN"
        expressions = (func.to_tsvector("english", text("||' '||".join(column_names))),)
    elif index_op == "unique":
        index_kwargs["unique"] = True
        expressions = [col for col in column_names if col in table.columns]
    else:
        # Try the operation on all columns and let sqlalchemy determine the error
        expressions = (getattr(func, index_op)(*column_names),)

    logger.info(f"create_index: creating index {index_name} on {table.name}")
    Index(index_name, *expressions, **index_kwargs).create()


def drop_index(table_or_name, index_name=None, column_names=None, ignore_errors=True):
    """
    Drops a database index by name for a specified table if it exists

    :param table_or_name: a sqlalchemy table object or the name of a table
    :param index_name: optionally specify an index name to drop
    :param column_names: optionally derive index name from a list or string of comma-seperated column names
    :param ignore_errors: if True, will log warning for database errors; otherwise will raise them

    If both index_name and column_names are provided, index_name takes precedence
    """

    if isinstance(table_or_name, str):
        table = get_tables().get(table_or_name)
    else:
        table = table_or_name

    if table is None:
        error = f"No table named {table_or_name}"
    elif column_names is None and index_name is None:
        error = "No index name provided"
    else:
        error = None

    if error:
        if not ignore_errors:
            raise ValueError(error)
        logger.warning(f"drop_index: {error}".lower())
        return

    if index_name is not None:
        column_index = None
    elif isinstance(column_names, str):
        column_index = "_".join(col.strip() for col in column_names.split(","))
    else:
        column_index = "_".join(column_names)

    if column_index is not None:
        index_name = f"{table.name}_{column_index}_idx"

    logger.info(f"drop_index: dropping index named {index_name}")

    try:
        Index(index_name, _table=table).drop()
    except exc.ProgrammingError:
        if not ignore_errors:
            raise
        logger.warning(f"drop_index: no index found named {index_name}")


def has_index(table_or_name, index_name):
    """
    Queries `pg_indexes` to see if an index exists, expression-based or otherwise, for a table.

    :param table_or_name: a sqlalchemy table object or the name of a table
    :param index_name: the name of the index to check for
    """

    if isinstance(table_or_name, str):
        table_name = table_or_name
    else:
        table_name = table_or_name.name

    index_query = (
        Select([literal_column("1")], distinct=True)
        .select_from(table("pg_indexes"))
        .where(and_(column("tablename") == table_name, column("indexname") == index_name))
    )

    with get_engine().connect() as conn:
        return bool(conn.execute(index_query).first())


def create_foreign_key(table_or_name, column_name, related_column_or_name):

    tables = get_tables()

    if isinstance(table_or_name, str):
        table = tables.get(table_or_name)
    else:
        table = table_or_name

    if isinstance(related_column_or_name, str):
        rel, col = related_column_or_name.split(".")
        related = getattr(tables.get(rel), "columns", {}).get(col)
    else:
        related = related_column_or_name

    validate_columns_in(table, [column_name], empty_table=table_or_name)

    if related is None:
        raise ValueError(f"No related column named {related_column_or_name}")
    else:
        col_name = f"{table.name}.{column_name}"
        rel_name = f"{related.table.name}.{related.name}"
        logger.info(f"create_foreign_key: adding FK to {col_name} on {rel_name}")

        ddl = AddConstraint(ForeignKeyConstraint([table.columns[column_name]], [related]))
        ddl.execute(table.bind, ddl.target)


def drop_foreign_key(table_or_name, fk_or_name):

    if isinstance(table_or_name, str):
        table = get_tables().get(table_or_name)
    else:
        table = table_or_name

    if table is None:
        raise ValueError(f"No table named {table_or_name}")

    if not isinstance(fk_or_name, str):
        fk = getattr(fk_or_name, "constraint", fk_or_name) or None
    else:
        fks = {fk.name.lower(): fk for fk in table.foreign_key_constraints}
        fk = fks.get(fk_or_name.lower())

    if fk is None:
        raise ValueError(f"No such foreign key in table {table.name}")

    logger.info(f"drop_foreign_key: dropping FK to {fk.name} from table {table.name}")

    ddl = DropConstraint(fk)
    ddl.execute(table.bind, ddl.target)
