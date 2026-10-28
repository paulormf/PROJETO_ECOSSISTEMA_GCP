"""Leitura paginada de tabelas PostgreSQL para o full inicial."""

from dataclasses import dataclass
from typing import Any, Iterator

from psycopg import Connection, sql
from psycopg.rows import dict_row


@dataclass(frozen=True, slots=True)
class TableSpec:
    """Metadados necessários para extrair uma tabela de forma determinística."""

    source_schema: str
    source_table: str
    columns: tuple[str, ...]
    primary_key: tuple[str, ...]

    def validate(self) -> None:
        """Valida o contrato mínimo da tabela antes de executar SQL."""

        if not self.columns:
            raise ValueError("columns não pode ser vazio")
        if not self.primary_key:
            raise ValueError("primary_key não pode ser vazio")
        if any(column not in self.columns for column in self.primary_key):
            raise ValueError("primary_key deve estar contida em columns")


CATEGORIAS_SPEC = TableSpec(
    source_schema="ecommerce",
    source_table="categorias",
    columns=("categoria_id", "categoria_pai_id", "nome_categoria", "criado_em"),
    primary_key=("categoria_id",),
)


def read_table_batches(
    connection: Connection,
    table: TableSpec,
    *,
    batch_size: int = 50_000,
    start_after: tuple[Any, ...] | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """Lê uma tabela em lotes usando paginação pela chave primária.

    A comparação de tuplas do PostgreSQL permite tratar chaves simples e
    compostas com a mesma lógica lexicográfica. O chamador controla a transação
    e pode persistir a última chave após concluir cada lote.
    """

    table.validate()
    if batch_size <= 0:
        raise ValueError("batch_size deve ser maior que zero")
    if start_after is not None and len(start_after) != len(table.primary_key):
        raise ValueError("start_after deve ter o mesmo tamanho da primary_key")

    last_key = start_after
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in table.columns)
    primary_key = sql.SQL(", ").join(
        sql.Identifier(column) for column in table.primary_key
    )
    order_by = sql.SQL(", ").join(
        sql.Identifier(column) for column in table.primary_key
    )
    placeholders = sql.SQL(", ").join(
        sql.Placeholder() for _ in table.primary_key
    )

    while True:
        parameters: list[Any] = []
        if last_key is None:
            where_clause = sql.SQL("")
        else:
            where_clause = sql.SQL("WHERE ({}) > ({})").format(
                primary_key,
                placeholders,
            )
            parameters.extend(last_key)

        query = sql.SQL(
            "SELECT {columns} "
            "FROM {schema}.{table} "
            "{where_clause} "
            "ORDER BY {order_by} "
            "LIMIT {limit}"
        ).format(
            columns=columns,
            schema=sql.Identifier(table.source_schema),
            table=sql.Identifier(table.source_table),
            where_clause=where_clause,
            order_by=order_by,
            limit=sql.Placeholder(),
        )
        parameters.append(batch_size)

        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, parameters)
            rows = cursor.fetchall()

        if not rows:
            return

        yield rows
        last_key = tuple(rows[-1][column] for column in table.primary_key)
