"""Escrita de lotes do full em Parquet no Cloud Storage."""

from datetime import date, datetime
from decimal import Decimal
from io import BytesIO
from typing import Any, Mapping, Sequence
from uuid import UUID
import json

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage


def write_full_batch(
    storage_client: storage.Client,
    *,
    bucket_name: str,
    object_name: str,
    rows: Sequence[Mapping[str, Any]],
    source_schema: str,
    source_table: str,
    run_id: str,
    snapshot_at: datetime,
    ingested_at: datetime,
) -> int:
    """Serializa um lote full e grava um objeto Parquet novo no GCS.

    O upload usa precondição de geração zero para impedir sobrescrita de um
    objeto já concluído. O lote deve ser limitado pelo chamador.
    """

    if not rows:
        return 0
    if not bucket_name.strip():
        raise ValueError("bucket_name não pode ser vazio")
    if not object_name.strip():
        raise ValueError("object_name não pode ser vazio")

    records = []
    for row in rows:
        record = {
            column: _normalize_value(value)
            for column, value in row.items()
        }
        record.update(
            {
                "source_system": "postgresql",
                "load_type": "full",
                "source_schema": source_schema,
                "source_table": source_table,
                "run_id": run_id,
                "snapshot_at": snapshot_at,
                "ingested_at": ingested_at,
            }
        )
        records.append(record)

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


def _normalize_value(value: Any) -> Any:
    """Converte tipos PostgreSQL que não possuem conversão Arrow direta."""

    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, default=_json_default)
    return value


def _json_default(value: Any) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Tipo não serializável em JSONB: {type(value).__name__}")
