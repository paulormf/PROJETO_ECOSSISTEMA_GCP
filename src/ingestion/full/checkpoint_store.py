"""Persistência do progresso do full load no BigQuery."""

from dataclasses import dataclass
from datetime import datetime, timezone

from google.cloud import bigquery

from ingestion.control.checkpoint_store import _quote_table_id


@dataclass(frozen=True, slots=True)
class FullLoadCheckpoint:
    """Estado persistido da extração de uma tabela."""

    pipeline_name: str
    source_system: str
    source_schema: str
    source_table: str
    run_id: str
    snapshot_at: datetime | None
    last_primary_key_json: str | None
    rows_extracted: int
    files_created: int
    status: str
    started_at: datetime
    completed_at: datetime | None
    error_message: str | None
    updated_at: datetime


class BigQueryFullLoadCheckpointStore:
    """Lê e grava o progresso do full por tabela."""

    def __init__(self, client: bigquery.Client, table_id: str) -> None:
        self._client = client
        self._table_id = _quote_table_id(table_id)

    def get(
        self,
        *,
        pipeline_name: str,
        source_system: str,
        source_schema: str,
        source_table: str,
    ) -> FullLoadCheckpoint | None:
        """Retorna o checkpoint da tabela ou ``None`` se ainda não existir."""

        query = f"""
        SELECT *
        FROM `{self._table_id}`
        WHERE pipeline_name = @pipeline_name
          AND source_system = @source_system
          AND source_schema = @source_schema
          AND source_table = @source_table
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "pipeline_name", "STRING", pipeline_name
                ),
                bigquery.ScalarQueryParameter(
                    "source_system", "STRING", source_system
                ),
                bigquery.ScalarQueryParameter(
                    "source_schema", "STRING", source_schema
                ),
                bigquery.ScalarQueryParameter(
                    "source_table", "STRING", source_table
                ),
            ]
        )
        row = next(iter(self._client.query(query, job_config=job_config)), None)
        if row is None:
            return None
        return FullLoadCheckpoint(**dict(row))

    def save(self, checkpoint: FullLoadCheckpoint) -> None:
        """Cria ou atualiza o checkpoint de uma tabela."""

        if checkpoint.rows_extracted < 0:
            raise ValueError("rows_extracted deve ser maior ou igual a zero")
        if checkpoint.files_created < 0:
            raise ValueError("files_created deve ser maior ou igual a zero")

        query = f"""
        MERGE `{self._table_id}` AS target
        USING (
            SELECT
                @pipeline_name AS pipeline_name,
                @source_system AS source_system,
                @source_schema AS source_schema,
                @source_table AS source_table,
                @run_id AS run_id,
                @snapshot_at AS snapshot_at,
                @last_primary_key_json AS last_primary_key_json,
                @rows_extracted AS rows_extracted,
                @files_created AS files_created,
                @status AS status,
                @started_at AS started_at,
                @completed_at AS completed_at,
                @error_message AS error_message,
                @updated_at AS updated_at
        ) AS source
        ON target.pipeline_name = source.pipeline_name
           AND target.source_system = source.source_system
           AND target.source_schema = source.source_schema
           AND target.source_table = source.source_table
        WHEN MATCHED THEN UPDATE SET
            run_id = source.run_id,
            snapshot_at = source.snapshot_at,
            last_primary_key_json = source.last_primary_key_json,
            rows_extracted = source.rows_extracted,
            files_created = source.files_created,
            status = source.status,
            started_at = source.started_at,
            completed_at = source.completed_at,
            error_message = source.error_message,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            pipeline_name,
            source_system,
            source_schema,
            source_table,
            run_id,
            snapshot_at,
            last_primary_key_json,
            rows_extracted,
            files_created,
            status,
            started_at,
            completed_at,
            error_message,
            updated_at
        ) VALUES (
            source.pipeline_name,
            source.source_system,
            source.source_schema,
            source.source_table,
            source.run_id,
            source.snapshot_at,
            source.last_primary_key_json,
            source.rows_extracted,
            source.files_created,
            source.status,
            source.started_at,
            source.completed_at,
            source.error_message,
            source.updated_at
        )
        """
        parameters = [
            bigquery.ScalarQueryParameter(
                "pipeline_name", "STRING", checkpoint.pipeline_name
            ),
            bigquery.ScalarQueryParameter(
                "source_system", "STRING", checkpoint.source_system
            ),
            bigquery.ScalarQueryParameter(
                "source_schema", "STRING", checkpoint.source_schema
            ),
            bigquery.ScalarQueryParameter(
                "source_table", "STRING", checkpoint.source_table
            ),
            bigquery.ScalarQueryParameter("run_id", "STRING", checkpoint.run_id),
            bigquery.ScalarQueryParameter(
                "snapshot_at", "TIMESTAMP", checkpoint.snapshot_at
            ),
            bigquery.ScalarQueryParameter(
                "last_primary_key_json", "STRING", checkpoint.last_primary_key_json
            ),
            bigquery.ScalarQueryParameter(
                "rows_extracted", "INT64", checkpoint.rows_extracted
            ),
            bigquery.ScalarQueryParameter(
                "files_created", "INT64", checkpoint.files_created
            ),
            bigquery.ScalarQueryParameter("status", "STRING", checkpoint.status),
            bigquery.ScalarQueryParameter(
                "started_at", "TIMESTAMP", checkpoint.started_at
            ),
            bigquery.ScalarQueryParameter(
                "completed_at", "TIMESTAMP", checkpoint.completed_at
            ),
            bigquery.ScalarQueryParameter(
                "error_message", "STRING", checkpoint.error_message
            ),
            bigquery.ScalarQueryParameter(
                "updated_at", "TIMESTAMP", datetime.now(timezone.utc)
            ),
        ]
        self._client.query(
            query,
            job_config=bigquery.QueryJobConfig(query_parameters=parameters),
        ).result()
