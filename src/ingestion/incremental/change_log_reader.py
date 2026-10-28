"""Leitura somente leitura do change log PostgreSQL."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, cast

from psycopg import Connection, connect
from psycopg.rows import dict_row

Operation = Literal["I", "U", "D"]

CHANGE_LOG_QUERY = """
SELECT
    change_id,
    occurred_at,
    operation,
    source_schema,
    source_table,
    primary_key,
    old_row,
    new_row,
    request_id
FROM audit.change_log
WHERE change_id > %s
ORDER BY change_id
LIMIT %s
"""


@dataclass(frozen=True, slots=True)
class ChangeEvent:
    """Evento de alteração retornado pelo change log."""

    change_id: int
    occurred_at: datetime
    operation: Operation
    source_schema: str
    source_table: str
    primary_key: dict[str, Any]
    old_row: dict[str, Any] | None
    new_row: dict[str, Any] | None
    request_id: str | None


def connect_read_only(database_url: str) -> Connection:
    """Abre uma conexão PostgreSQL configurada como somente leitura."""

    if not database_url.strip():
        raise ValueError("database_url não pode ser vazio")

    return connect(
        database_url,
        row_factory=dict_row,
        options="-c default_transaction_read_only=on",
    )


def read_change_log(
    connection: Connection,
    *,
    after_change_id: int,
    page_size: int = 1_000,
) -> list[ChangeEvent]:
    """Lê uma página ordenada de eventos após o watermark informado."""

    if after_change_id < 0:
        raise ValueError("after_change_id deve ser maior ou igual a zero")
    if page_size <= 0:
        raise ValueError("page_size deve ser maior que zero")

    with connection.cursor() as cursor:
        cursor.execute(CHANGE_LOG_QUERY, (after_change_id, page_size))
        rows = cursor.fetchall()

    return [_to_change_event(row) for row in rows]


def _to_change_event(row: dict[str, Any]) -> ChangeEvent:
    """Converte uma linha do driver em um evento tipado."""

    return ChangeEvent(
        change_id=cast(int, row["change_id"]),
        occurred_at=cast(datetime, row["occurred_at"]),
        operation=cast(Operation, row["operation"]),
        source_schema=cast(str, row["source_schema"]),
        source_table=cast(str, row["source_table"]),
        primary_key=cast(dict[str, Any], row["primary_key"]),
        old_row=cast(dict[str, Any] | None, row["old_row"]),
        new_row=cast(dict[str, Any] | None, row["new_row"]),
        request_id=cast(str | None, row["request_id"]),
    )
