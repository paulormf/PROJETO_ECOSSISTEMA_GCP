"""Orquestra o full de uma tabela PostgreSQL para o Cloud Storage."""

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import json
from typing import Any
from uuid import uuid4

from google.cloud import storage
from psycopg import Connection

from ingestion.full.load_control_store import (
    BigQueryFullLoadControlStore,
    FullLoadFile,
    FullLoadRun,
)
from ingestion.full.reader import TableSpec, read_table_batches
from ingestion.landing.parquet_writer import write_full_batch


@dataclass(frozen=True, slots=True)
class FullLoadResult:
    """Resumo da execução do full de uma tabela."""

    source_table: str
    logical_load_id: str
    attempt_id: str
    rows_extracted: int
    files_created: int
    status: str


def run_full_table(
    connection: Connection,
    storage_client: storage.Client,
    control_store: BigQueryFullLoadControlStore,
    table: TableSpec,
    *,
    bucket_name: str,
    pipeline_name: str = "postgres-full-load",
    source_system: str = "postgresql",
    batch_size: int = 50_000,
) -> FullLoadResult:
    """Executa ou retoma o full de uma tabela e registra seus arquivos."""

    table.validate()
    existing = control_store.get_latest_run(
        pipeline_name=pipeline_name,
        source_system=source_system,
        source_schema=table.source_schema,
        source_table=table.source_table,
    )
    if existing is not None and existing.status == "COMPLETED":
        return FullLoadResult(
            source_table=table.source_table,
            logical_load_id=existing.logical_load_id,
            attempt_id="",
            rows_extracted=existing.extracted_rows,
            files_created=existing.extracted_files,
            status=existing.status,
        )

    start_after = _decode_last_key(existing, table)
    logical_load_id = existing.logical_load_id if existing is not None else str(uuid4())
    attempt_id = str(uuid4())
    started_at = existing.started_at if existing is not None else datetime.now(timezone.utc)
    snapshot_at = existing.snapshot_at if existing is not None else started_at
    rows_extracted = existing.extracted_rows if existing is not None else 0
    files_created = existing.extracted_files if existing is not None else 0
    run = FullLoadRun(
        logical_load_id=logical_load_id,
        pipeline_name=pipeline_name,
        source_system=source_system,
        source_schema=table.source_schema,
        source_table=table.source_table,
        snapshot_at=snapshot_at,
        last_primary_key_json=existing.last_primary_key_json if existing is not None else None,
        status="RUNNING",
        expected_rows=None,
        expected_files=None,
        extracted_rows=rows_extracted,
        extracted_files=files_created,
        started_at=started_at,
        completed_at=None,
        error_message=None,
        updated_at=started_at,
    )
    control_store.save_run(run)

    try:
        for rows in read_table_batches(
            connection,
            table,
            batch_size=batch_size,
            start_after=start_after,
        ):
            object_name = _object_name(
                table=table,
                run_id=attempt_id,
                part_number=files_created,
            )
            write_full_batch(
                storage_client,
                bucket_name=bucket_name,
                object_name=object_name,
                rows=rows,
                source_schema=table.source_schema,
                source_table=table.source_table,
                run_id=attempt_id,
                snapshot_at=snapshot_at,
                ingested_at=datetime.now(timezone.utc),
            )

            rows_extracted += len(rows)
            files_created += 1
            last_key = tuple(rows[-1][column] for column in table.primary_key)
            updated_at = datetime.now(timezone.utc)
            control_store.save_file(
                FullLoadFile(
                    logical_load_id=logical_load_id,
                    attempt_id=attempt_id,
                    source_system=source_system,
                    source_schema=table.source_schema,
                    source_table=table.source_table,
                    object_name=object_name,
                    row_count=len(rows),
                    status="EXTRACTED",
                    created_at=updated_at,
                )
            )
            run = replace(
                run,
                last_primary_key_json=json.dumps(last_key, default=str),
                extracted_rows=rows_extracted,
                extracted_files=files_created,
                updated_at=updated_at,
            )
            control_store.save_run(run)

        run = replace(
            run,
            status="COMPLETED",
            completed_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        control_store.save_run(run)
    except Exception as error:
        run = replace(
            run,
            status="FAILED",
            error_message=str(error)[:4_000],
            updated_at=datetime.now(timezone.utc),
        )
        control_store.save_run(run)
        raise

    return FullLoadResult(
        source_table=table.source_table,
        logical_load_id=logical_load_id,
        attempt_id=attempt_id,
        rows_extracted=rows_extracted,
        files_created=files_created,
        status="COMPLETED",
    )


def _decode_last_key(
    checkpoint: FullLoadRun | None,
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
