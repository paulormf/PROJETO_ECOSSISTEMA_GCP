"""Orquestra a leitura do change log e a gravação incremental no GCS."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import DefaultDict
from uuid import uuid4

from google.cloud import storage
from psycopg import Connection

from ingestion.control.checkpoint_store import BigQueryCheckpointStore
from ingestion.incremental.change_log_reader import (
    ChangeEvent,
    read_change_log,
)
from ingestion.incremental.parquet_writer import (
    incremental_object_name,
    write_incremental_batch,
)


@dataclass(frozen=True, slots=True)
class IncrementalRunResult:
    """Resumo de uma execução incremental."""

    run_id: str
    last_change_id: int
    events_processed: int
    files_created: int
    status: str


def run_incremental(
    connection: Connection,
    storage_client: storage.Client,
    checkpoint_store: BigQueryCheckpointStore,
    *,
    bucket_name: str,
    pipeline_name: str = "postgres-incremental",
    source_system: str = "postgresql",
    page_size: int = 1_000,
) -> IncrementalRunResult:
    """Processa eventos após o checkpoint e grava-os no GCS em páginas."""

    existing = checkpoint_store.get(
        pipeline_name=pipeline_name,
        source_system=source_system,
    )
    last_change_id = existing.last_change_id if existing is not None else 0
    run_id = str(uuid4())
    started_at = datetime.now(timezone.utc)
    checkpoint_store.save(
        pipeline_name=pipeline_name,
        source_system=source_system,
        last_change_id=last_change_id,
        last_run_id=run_id,
        status="RUNNING",
        last_processed_at=existing.last_processed_at if existing else None,
    )

    events_processed = 0
    files_created = 0
    part_number = 0

    try:
        while True:
            events = read_change_log(
                connection,
                after_change_id=last_change_id,
                page_size=page_size,
            )
            if not events:
                break

            grouped_events = _group_by_table(events)
            for table_events in grouped_events.values():
                event = table_events[0]
                object_name = incremental_object_name(
                    source_schema=event.source_schema,
                    source_table=event.source_table,
                    run_id=run_id,
                    part_number=part_number,
                )
                write_incremental_batch(
                    storage_client,
                    bucket_name=bucket_name,
                    object_name=object_name,
                    events=table_events,
                    run_id=run_id,
                    ingested_at=datetime.now(timezone.utc),
                )
                files_created += 1

            last_event = events[-1]
            last_change_id = last_event.change_id
            events_processed += len(events)
            part_number += 1
            checkpoint_store.save(
                pipeline_name=pipeline_name,
                source_system=source_system,
                last_change_id=last_change_id,
                last_processed_at=last_event.occurred_at,
                last_run_id=run_id,
                status="RUNNING",
            )

        checkpoint_store.save(
            pipeline_name=pipeline_name,
            source_system=source_system,
            last_change_id=last_change_id,
            last_processed_at=(
                datetime.now(timezone.utc) if events_processed else existing.last_processed_at
                if existing
                else started_at
            ),
            last_run_id=run_id,
            status="COMPLETED",
        )
    except Exception as error:
        checkpoint_store.save(
            pipeline_name=pipeline_name,
            source_system=source_system,
            last_change_id=last_change_id,
            last_processed_at=existing.last_processed_at if existing else None,
            last_run_id=run_id,
            status="FAILED",
            error_message=str(error)[:4_000],
        )
        raise

    return IncrementalRunResult(
        run_id=run_id,
        last_change_id=last_change_id,
        events_processed=events_processed,
        files_created=files_created,
        status="COMPLETED",
    )


def _group_by_table(
    events: list[ChangeEvent],
) -> DefaultDict[tuple[str, str], list[ChangeEvent]]:
    grouped: DefaultDict[tuple[str, str], list[ChangeEvent]] = defaultdict(list)
    for event in events:
        grouped[(event.source_schema, event.source_table)].append(event)
    return grouped
