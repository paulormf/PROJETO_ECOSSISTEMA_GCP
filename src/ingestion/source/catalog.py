"""Descoberta controlada do catálogo de tabelas da origem PostgreSQL."""

from collections.abc import Iterable

from psycopg import Connection, sql

from ingestion.full.reader import TableSpec


def discover_table_spec(
    connection: Connection,
    *,
    source_schema: str,
    source_table: str,
) -> TableSpec:
    """Descobre colunas e chave primária de uma tabela PostgreSQL."""

    column_query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """
    primary_key_query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON kcu.constraint_schema = tc.constraint_schema
         AND kcu.constraint_name = tc.constraint_name
         AND kcu.table_schema = tc.table_schema
         AND kcu.table_name = tc.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = %s
          AND tc.table_name = %s
        ORDER BY kcu.ordinal_position
    """

    with connection.cursor() as cursor:
        cursor.execute(column_query, (source_schema, source_table))
        columns = tuple(row[0] for row in cursor.fetchall())

        cursor.execute(primary_key_query, (source_schema, source_table))
        primary_key = tuple(row[0] for row in cursor.fetchall())

    if not columns:
        raise ValueError(
            f"Tabela não encontrada ou sem colunas: "
            f"{source_schema}.{source_table}"
        )
    if not primary_key:
        raise ValueError(
            f"Tabela sem chave primária suportada: "
            f"{source_schema}.{source_table}"
        )

    table = TableSpec(
        source_schema=source_schema,
        source_table=source_table,
        columns=columns,
        primary_key=primary_key,
    )
    table.validate()
    return table


def discover_table_specs(
    connection: Connection,
    *,
    source_schema: str,
    include_tables: Iterable[str] | None = None,
) -> tuple[TableSpec, ...]:
    """Lista tabelas de negócio e descobre seus contratos de extração."""

    table_query = sql.SQL(
        "SELECT table_name "
        "FROM information_schema.tables "
        "WHERE table_schema = %s "
        "AND table_type = 'BASE TABLE' "
        "ORDER BY table_name"
    )
    with connection.cursor() as cursor:
        cursor.execute(table_query, (source_schema,))
        available_tables = tuple(row[0] for row in cursor.fetchall())

    requested_tables = (
        tuple(include_tables) if include_tables is not None else available_tables
    )
    unknown_tables = sorted(set(requested_tables) - set(available_tables))
    if unknown_tables:
        raise ValueError(
            f"Tabelas não encontradas em {source_schema}: "
            f"{', '.join(unknown_tables)}"
        )

    return tuple(
        discover_table_spec(
            connection,
            source_schema=source_schema,
            source_table=source_table,
        )
        for source_table in requested_tables
    )
