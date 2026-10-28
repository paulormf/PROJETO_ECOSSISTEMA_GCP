"""Carrega eventos incrementais do GCS para a tabela técnica da Bronze."""

from dataclasses import dataclass
import re
from uuid import uuid4

from google.cloud import bigquery, storage


_IDENTIFIER = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True, slots=True)
class IncrementalBronzeLoadResult:
    """Resumo do carregamento de um run incremental."""

    run_id: str
    files_found: int
    rows_loaded_to_stage: int
    rows_merged: int
    load_job_id: str
    merge_job_id: str
    destination_table: str


def load_incremental_run_to_events(
    storage_client: storage.Client,
    bigquery_client: bigquery.Client,
    *,
    bucket_name: str,
    run_id: str,
    destination_project: str,
    destination_dataset: str,
    location: str = "us-central1",
) -> IncrementalBronzeLoadResult:
    """Carrega um run do GCS na tabela técnica com MERGE idempotente."""

    if not bucket_name.strip():
        raise ValueError("bucket_name não pode ser vazio")
    if not run_id.strip():
        raise ValueError("run_id não pode ser vazio")
    _validate_identifier(destination_project, "destination_project")
    _validate_identifier(destination_dataset, "destination_dataset")

    blobs = storage_client.list_blobs(bucket_name, prefix="source=postgresql/")
    object_names = sorted(
        blob.name
        for blob in blobs
        if f"/run_id={run_id}/" in blob.name
        and blob.name.endswith(".parquet")
        and "/load_type=incremental/" in blob.name
    )
    if not object_names:
        raise FileNotFoundError(f"Nenhum Parquet incremental encontrado para {run_id}")

    destination_table = (
        f"{destination_project}.{destination_dataset}.bronze_incremental_events"
    )
    stage_table = (
        f"{destination_project}.{destination_dataset}._stage_incremental_events_"
        f"{uuid4().hex}"
    )
    source_uris = [f"gs://{bucket_name}/{object_name}" for object_name in object_names]

    load_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=_event_schema(),
        ignore_unknown_values=False,
    )

    try:
        load_job = bigquery_client.load_table_from_uri(
            source_uris,
            stage_table,
            job_config=load_config,
            location=location,
        )
        load_job.result()

        merge_job = bigquery_client.query(
            _merge_events_query(
                destination_table=destination_table,
                stage_table=stage_table,
            ),
            job_config=bigquery.QueryJobConfig(),
            location=location,
        )
        merge_job.result()

        return IncrementalBronzeLoadResult(
            run_id=run_id,
            files_found=len(object_names),
            rows_loaded_to_stage=int(load_job.output_rows or 0),
            rows_merged=int(merge_job.num_dml_affected_rows or 0),
            load_job_id=load_job.job_id,
            merge_job_id=merge_job.job_id,
            destination_table=destination_table,
        )
    finally:
        bigquery_client.delete_table(stage_table, not_found_ok=True)


def _event_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("source_system", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("load_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("change_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("occurred_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("operation", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_schema", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("primary_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("old_row", "STRING"),
        bigquery.SchemaField("new_row", "STRING"),
        bigquery.SchemaField("request_id", "STRING"),
        bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    ]


def _merge_events_query(*, destination_table: str, stage_table: str) -> str:
    return f"""
    MERGE `{destination_table}` AS target
    USING `{stage_table}` AS stage
    ON target.source_system = stage.source_system
       AND target.change_id = stage.change_id
    WHEN MATCHED THEN UPDATE SET
        source_system = stage.source_system,
        load_type = stage.load_type,
        change_id = stage.change_id,
        occurred_at = stage.occurred_at,
        operation = stage.operation,
        source_schema = stage.source_schema,
        source_table = stage.source_table,
        primary_key = stage.primary_key,
        old_row = stage.old_row,
        new_row = stage.new_row,
        request_id = stage.request_id,
        run_id = stage.run_id,
        ingested_at = stage.ingested_at
    WHEN NOT MATCHED THEN INSERT (
        source_system,
        load_type,
        change_id,
        occurred_at,
        operation,
        source_schema,
        source_table,
        primary_key,
        old_row,
        new_row,
        request_id,
        run_id,
        ingested_at
    ) VALUES (
        stage.source_system,
        stage.load_type,
        stage.change_id,
        stage.occurred_at,
        stage.operation,
        stage.source_schema,
        stage.source_table,
        stage.primary_key,
        stage.old_row,
        stage.new_row,
        stage.request_id,
        stage.run_id,
        stage.ingested_at
    )
    """


def _validate_identifier(value: str, name: str) -> None:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} contém caracteres inválidos")
