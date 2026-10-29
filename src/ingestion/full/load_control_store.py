"""Controle do full load por carga lógica, tentativa e arquivo."""

from dataclasses import dataclass
from datetime import datetime

from google.cloud import bigquery

from ingestion.control.checkpoint_store import _quote_table_id


@dataclass(frozen=True, slots=True)
class FullLoadRun:
    """Estado acumulado de uma carga full lógica."""

    logical_load_id: str
    pipeline_name: str
    source_system: str
    source_schema: str
    source_table: str
    snapshot_at: datetime
    last_primary_key_json: str | None
    status: str
    expected_rows: int | None
    expected_files: int | None
    extracted_rows: int
    extracted_files: int
    started_at: datetime
    completed_at: datetime | None
    error_message: str | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class FullLoadFile:
    """Metadados de um Parquet gerado por uma tentativa."""

    logical_load_id: str
    attempt_id: str
    source_system: str
    source_schema: str
    source_table: str
    object_name: str
    row_count: int
    status: str
    created_at: datetime
    loaded_at: datetime | None = None
    error_message: str | None = None


class BigQueryFullLoadControlStore:
    """Persiste o estado do full load nas tabelas de controle do BigQuery."""

    def __init__(self, client: bigquery.Client, dataset_id: str) -> None:
        self._client = client
        self._runs_table = _quote_table_id(f"{dataset_id}.full_load_runs")
        self._files_table = _quote_table_id(f"{dataset_id}.full_load_files")

    def get_latest_run(
        self,
        *,
        pipeline_name: str,
        source_system: str,
        source_schema: str,
        source_table: str,
    ) -> FullLoadRun | None:
        """Retorna a carga lógica mais recente da tabela de origem."""

        query = f"""
        SELECT *
        FROM `{self._runs_table}`
        WHERE pipeline_name = @pipeline_name
          AND source_system = @source_system
          AND source_schema = @source_schema
          AND source_table = @source_table
        QUALIFY ROW_NUMBER() OVER (ORDER BY started_at DESC) = 1
        """
        parameters = [
            bigquery.ScalarQueryParameter("pipeline_name", "STRING", pipeline_name),
            bigquery.ScalarQueryParameter("source_system", "STRING", source_system),
            bigquery.ScalarQueryParameter("source_schema", "STRING", source_schema),
            bigquery.ScalarQueryParameter("source_table", "STRING", source_table),
        ]
        row = next(
            iter(
                self._client.query(
                    query,
                    job_config=bigquery.QueryJobConfig(query_parameters=parameters),
                )
            ),
            None,
        )
        return FullLoadRun(**dict(row)) if row is not None else None

    def save_run(self, run: FullLoadRun) -> None:
        """Insere ou atualiza uma carga lógica."""

        query = f"""
        MERGE `{self._runs_table}` AS target
        USING (
          SELECT
            @logical_load_id AS logical_load_id,
            @pipeline_name AS pipeline_name,
            @source_system AS source_system,
            @source_schema AS source_schema,
            @source_table AS source_table,
            @snapshot_at AS snapshot_at,
            @last_primary_key_json AS last_primary_key_json,
            @status AS status,
            @expected_rows AS expected_rows,
            @expected_files AS expected_files,
            @extracted_rows AS extracted_rows,
            @extracted_files AS extracted_files,
            @started_at AS started_at,
            @completed_at AS completed_at,
            @error_message AS error_message,
            @updated_at AS updated_at
        ) AS source
        ON target.logical_load_id = source.logical_load_id
        WHEN MATCHED THEN UPDATE SET
          status = source.status,
          last_primary_key_json = source.last_primary_key_json,
          expected_rows = source.expected_rows,
          expected_files = source.expected_files,
          extracted_rows = source.extracted_rows,
          extracted_files = source.extracted_files,
          completed_at = source.completed_at,
          error_message = source.error_message,
          updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
          logical_load_id,
          pipeline_name,
          source_system,
          source_schema,
          source_table,
          snapshot_at,
          status,
          expected_rows,
          expected_files,
          extracted_rows,
          extracted_files,
          started_at,
          completed_at,
          error_message,
          updated_at,
          last_primary_key_json
        ) VALUES (
          source.logical_load_id,
          source.pipeline_name,
          source.source_system,
          source.source_schema,
          source.source_table,
          source.snapshot_at,
          source.status,
          source.expected_rows,
          source.expected_files,
          source.extracted_rows,
          source.extracted_files,
          source.started_at,
          source.completed_at,
          source.error_message,
          source.updated_at,
          source.last_primary_key_json
        )
        """
        self._client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("logical_load_id", "STRING", run.logical_load_id),
                    bigquery.ScalarQueryParameter("pipeline_name", "STRING", run.pipeline_name),
                    bigquery.ScalarQueryParameter("source_system", "STRING", run.source_system),
                    bigquery.ScalarQueryParameter("source_schema", "STRING", run.source_schema),
                    bigquery.ScalarQueryParameter("source_table", "STRING", run.source_table),
                    bigquery.ScalarQueryParameter("snapshot_at", "TIMESTAMP", run.snapshot_at),
                    bigquery.ScalarQueryParameter(
                        "last_primary_key_json", "STRING", run.last_primary_key_json
                    ),
                    bigquery.ScalarQueryParameter("status", "STRING", run.status),
                    bigquery.ScalarQueryParameter("expected_rows", "INT64", run.expected_rows),
                    bigquery.ScalarQueryParameter("expected_files", "INT64", run.expected_files),
                    bigquery.ScalarQueryParameter("extracted_rows", "INT64", run.extracted_rows),
                    bigquery.ScalarQueryParameter("extracted_files", "INT64", run.extracted_files),
                    bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", run.started_at),
                    bigquery.ScalarQueryParameter("completed_at", "TIMESTAMP", run.completed_at),
                    bigquery.ScalarQueryParameter("error_message", "STRING", run.error_message),
                    bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", run.updated_at),
                ]
            ),
        ).result()

    def save_file(self, file: FullLoadFile) -> None:
        """Registra um Parquet de forma idempotente pelo nome do objeto."""

        query = f"""
        MERGE `{self._files_table}` AS target
        USING (
          SELECT
            @logical_load_id AS logical_load_id,
            @attempt_id AS attempt_id,
            @source_system AS source_system,
            @source_schema AS source_schema,
            @source_table AS source_table,
            @object_name AS object_name,
            @row_count AS row_count,
            @status AS status,
            @created_at AS created_at,
            @loaded_at AS loaded_at,
            @error_message AS error_message
        ) AS source
        ON target.logical_load_id = source.logical_load_id
       AND target.object_name = source.object_name
        WHEN MATCHED THEN UPDATE SET
          status = source.status,
          loaded_at = source.loaded_at,
          error_message = source.error_message
        WHEN NOT MATCHED THEN INSERT ROW
        """
        self._client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("logical_load_id", "STRING", file.logical_load_id),
                    bigquery.ScalarQueryParameter("attempt_id", "STRING", file.attempt_id),
                    bigquery.ScalarQueryParameter("source_system", "STRING", file.source_system),
                    bigquery.ScalarQueryParameter("source_schema", "STRING", file.source_schema),
                    bigquery.ScalarQueryParameter("source_table", "STRING", file.source_table),
                    bigquery.ScalarQueryParameter("object_name", "STRING", file.object_name),
                    bigquery.ScalarQueryParameter("row_count", "INT64", file.row_count),
                    bigquery.ScalarQueryParameter("status", "STRING", file.status),
                    bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", file.created_at),
                    bigquery.ScalarQueryParameter("loaded_at", "TIMESTAMP", file.loaded_at),
                    bigquery.ScalarQueryParameter("error_message", "STRING", file.error_message),
                ]
            ),
        ).result()

    def list_extracted_files(self, logical_load_id: str) -> tuple[str, ...]:
        """Retorna todos os objetos extraídos da carga lógica, sem duplicidade."""

        query = f"""
        SELECT object_name
        FROM `{self._files_table}`
        WHERE logical_load_id = @logical_load_id
          AND status IN ('EXTRACTED', 'LOADED')
        ORDER BY object_name
        """
        rows = self._client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "logical_load_id", "STRING", logical_load_id
                    )
                ]
            ),
        )
        return tuple(row["object_name"] for row in rows)

    def mark_files_loaded(self, logical_load_id: str) -> None:
        """Marca como carregados os arquivos da carga lógica."""

        query = f"""
        UPDATE `{self._files_table}`
        SET status = 'LOADED', loaded_at = CURRENT_TIMESTAMP()
        WHERE logical_load_id = @logical_load_id
          AND status = 'EXTRACTED'
        """
        self._client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "logical_load_id", "STRING", logical_load_id
                    )
                ]
            ),
        ).result()
