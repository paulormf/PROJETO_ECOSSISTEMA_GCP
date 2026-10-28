"""Escrita de eventos do change log em Parquet no Cloud Storage."""

from datetime import date, datetime, timezone
import json
from io import BytesIO
from typing import Any, Sequence
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

from ingestion.incremental.change_log_reader import ChangeEvent


def write_incremental_batch(
    storage_client: storage.Client,
    *,
    bucket_name: str,
    object_name: str,
    events: Sequence[ChangeEvent],
    run_id: str,
    ingested_at: datetime,
) -> int:
    """Serializa uma página homogênea do change log em Parquet."""

    if not events:
        return 0
    if not bucket_name.strip():
        raise ValueError("bucket_name não pode ser vazio")
    if not object_name.strip():
        raise ValueError("object_name não pode ser vazio")

    source_schema = events[0].source_schema
    source_table = events[0].source_table
    if any(
        event.source_schema != source_schema or event.source_table != source_table
        for event in events
    ):
        raise ValueError("todos os eventos do lote devem pertencer à mesma tabela")

    records = [
        {
            "source_system": "postgresql",
            "load_type": "incremental",
            "change_id": event.change_id,
            "occurred_at": event.occurred_at,
            "operation": event.operation,
            "source_schema": event.source_schema,
            "source_table": event.source_table,
            "primary_key": _json_text(event.primary_key),
            "old_row": _json_text(event.old_row),
            "new_row": _json_text(event.new_row),
            "request_id": event.request_id,
            "run_id": run_id,
            "ingested_at": ingested_at,
        }
        for event in events
    ]

    table = pa.Table.from_pylist(records)
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    object_size = buffer.tell()
    buffer.seek(0)

    blob = storage_client.bucket(bucket_name).blob(object_name)
    blob.upload_from_file(
        buffer,
        rewind=True,
        content_type="application/octet-stream",
        if_generation_match=0,
    )
    return object_size


def incremental_object_name(
    *,
    source_schema: str,
    source_table: str,
    run_id: str,
    part_number: int,
    ingestion_date: date | None = None,
) -> str:
    """Monta o caminho padronizado de um arquivo incremental."""

    if part_number < 0:
        raise ValueError("part_number deve ser maior ou igual a zero")
    effective_date = ingestion_date or datetime.now(timezone.utc).date()
    return (
        f"source=postgresql/schema={source_schema}/"
        f"table={source_table}/load_type=incremental/"
        f"ingestion_date={effective_date.isoformat()}/run_id={run_id}/"
        f"part-{part_number:05d}.parquet"
    )


def _json_text(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        default=_json_default,
    )


def _json_default(value: Any) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)
