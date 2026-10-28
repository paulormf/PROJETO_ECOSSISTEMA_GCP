"""Orquestra o full de uma tabela PostgreSQL para o Cloud Storage."""

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import json
from typing import Any
from uuid import uuid4

from google.cloud import storage
from psycopg import Connection

from ingestion.full.checkpoint_store import (
    BigQueryFullLoadCheckpointStore,
    FullLoadCheckpoint,
)
from ingestion.full.reader import TableSpec, read_table_batches
from ingestion.landing.parquet_writer import write_full_batch


@dataclass(frozen=True, slots=True)
class FullLoadResult:
    """Resumo da execução do full de uma tabela."""

    source_table: str
    run_id: str
    rows_extracted: int
    files_created: int
    status: str


def run_full_table(
    connection: Connection,
    storage_client: storage.Client,
    checkpoint_store: BigQueryFullLoadCheckpointStore,
    table: TableSpec,
    *,
    bucket_name: str,
    pipeline_name: str = "postgres-full-load",
    source_system: str = "postgresql",
    batch_size: int = 50_000,
) -> FullLoadResult:
    """Executa ou retoma o full de uma tabela e atualiza seu checkpoint."""

    table.validate()
    existing = checkpoint_store.get(
        pipeline_name=pipeline_name,
        source_system=source_system,
        source_schema=table.source_schema,
        source_table=table.source_table,
    )
    if existing is not None and existing.status == "COMPLETED":
        return FullLoadResult(
            source_table=table.source_table,
            run_id=existing.run_id,
            rows_extracted=existing.rows_extracted,
            files_created=existing.files_created,
            status=existing.status,
        )

    start_after = _decode_last_key(existing, table)
    run_id = str(uuid4())
    started_at = datetime.now(timezone.utc)
    snapshot_at = started_at
    rows_extracted = existing.rows_extracted if existing is not None else 0
    files_created = existing.files_created if existing is not None else 0
    checkpoint = FullLoadCheckpoint(
        pipeline_name=pipeline_name,
        source_system=source_system,
        source_schema=table.source_schema,
        source_table=table.source_table,
        run_id=run_id,
        snapshot_at=snapshot_at,
        last_primary_key_json=existing.last_primary_key_json
        if existing is not None
        else None,
        rows_extracted=rows_extracted,
        files_created=files_created,
        status="RUNNING",
        started_at=started_at,
        completed_at=None,
        error_message=None,
        updated_at=started_at,
    )
    checkpoint_store.save(checkpoint)

    try:
        for rows in read_table_batches(
            connection,
            table,
            batch_size=batch_size,
            start_after=start_after,
        ):
            object_name = _object_name(
                table=table,
                run_id=run_id,
                part_number=files_created,
            )
            write_full_batch(
                storage_client,
                bucket_name=bucket_name,
                object_name=object_name,
                rows=rows,
                source_schema=table.source_schema,
                source_table=table.source_table,
                run_id=run_id,
                snapshot_at=snapshot_at,
                ingested_at=datetime.now(timezone.utc),
            )

            rows_extracted += len(rows)
            files_created += 1
            last_key = tuple(rows[-1][column] for column in table.primary_key)
            checkpoint = replace(
                checkpoint,
                last_primary_key_json=json.dumps(last_key, default=str),
                rows_extracted=rows_extracted,
                files_created=files_created,
                updated_at=datetime.now(timezone.utc),
            )
            checkpoint_store.save(checkpoint)

        checkpoint = replace(
            checkpoint,
            status="COMPLETED",
            completed_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        checkpoint_store.save(checkpoint)
    except Exception as error:
        checkpoint = replace(
            checkpoint,
            status="FAILED",
            error_message=str(error)[:4_000],
            updated_at=datetime.now(timezone.utc),
        )
        checkpoint_store.save(checkpoint)
        raise

    return FullLoadResult(
        source_table=table.source_table,
        run_id=run_id,
        rows_extracted=rows_extracted,
        files_created=files_created,
        status="COMPLETED",
    )


def _decode_last_key(
    checkpoint: FullLoadCheckpoint | None,
    table: TableSpec,
) -> tuple[Any, ...] | None:
    if checkpoint is None or checkpoint.last_primary_key_json is None:
        return None

    values = json.loads(checkpoint.last_primary_key_json)
    if not isinstance(values, list) or len(values) != len(table.primary_key):
        raise ValueError("last_primary_key_json incompatível com a chave da tabela")
    return tuple(values)


def _object_name(*, table: TableSpec, run_id: str, part_number: int) -> str:
    ingestion_date = datetime.now(timezone.utc).date().isoformat()
    return (
        f"source=postgresql/schema={table.source_schema}/"
        f"table={table.source_table}/load_type=full/"
        f"ingestion_date={ingestion_date}/run_id={run_id}/"
        f"part-{part_number:05d}.parquet"
    )
