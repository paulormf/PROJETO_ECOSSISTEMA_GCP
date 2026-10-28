"""Persistência do checkpoint de ingestão no BigQuery."""

from dataclasses import dataclass
from datetime import datetime, timezone
import re

from google.cloud import bigquery


@dataclass(frozen=True, slots=True)
class Checkpoint:
    """Estado persistido de uma pipeline incremental."""

    pipeline_name: str
    source_system: str
    last_change_id: int
    last_processed_at: datetime | None
    last_run_id: str | None
    status: str
    error_message: str | None
    updated_at: datetime


class BigQueryCheckpointStore:
    """Lê e grava checkpoints em uma tabela de controle do BigQuery."""

    def __init__(self, client: bigquery.Client, table_id: str) -> None:
        self._client = client
        self._table_id = _quote_table_id(table_id)

    def get(self, *, pipeline_name: str, source_system: str) -> Checkpoint | None:
        """Retorna o checkpoint da pipeline ou ``None`` se ainda não existir."""

        query = f"""
        SELECT
            pipeline_name,
            source_system,
            last_change_id,
            last_processed_at,
            last_run_id,
            status,
            error_message,
            updated_at
        FROM `{self._table_id}`
        WHERE pipeline_name = @pipeline_name
          AND source_system = @source_system
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
            ]
        )
        row = next(iter(self._client.query(query, job_config=job_config)), None)
        if row is None:
            return None
        return Checkpoint(**dict(row))

    def save(
        self,
        *,
        pipeline_name: str,
        source_system: str,
        last_change_id: int,
        status: str,
        last_run_id: str | None = None,
        last_processed_at: datetime | None = None,
        error_message: str | None = None,
    ) -> None:
        """Cria ou atualiza o checkpoint da pipeline."""

        if last_change_id < 0:
            raise ValueError("last_change_id deve ser maior ou igual a zero")

        query = f"""
        MERGE `{self._table_id}` AS target
        USING (
            SELECT
                @pipeline_name AS pipeline_name,
                @source_system AS source_system,
                @last_change_id AS last_change_id,
                @last_processed_at AS last_processed_at,
                @last_run_id AS last_run_id,
                @status AS status,
                @error_message AS error_message,
                @updated_at AS updated_at
        ) AS source
        ON target.pipeline_name = source.pipeline_name
           AND target.source_system = source.source_system
        WHEN MATCHED THEN UPDATE SET
            last_change_id = source.last_change_id,
            last_processed_at = source.last_processed_at,
            last_run_id = source.last_run_id,
            status = source.status,
            error_message = source.error_message,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            pipeline_name,
            source_system,
            last_change_id,
            last_processed_at,
            last_run_id,
            status,
            error_message,
            updated_at
        ) VALUES (
            source.pipeline_name,
            source.source_system,
            source.last_change_id,
            source.last_processed_at,
            source.last_run_id,
            source.status,
            source.error_message,
            source.updated_at
        )
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
                    "last_change_id", "INT64", last_change_id
                ),
                bigquery.ScalarQueryParameter(
                    "last_processed_at", "TIMESTAMP", last_processed_at
                ),
                bigquery.ScalarQueryParameter("last_run_id", "STRING", last_run_id),
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter(
                    "error_message", "STRING", error_message
                ),
                bigquery.ScalarQueryParameter(
                    "updated_at", "TIMESTAMP", datetime.now(timezone.utc)
                ),
            ]
        )
        self._client.query(query, job_config=job_config).result()


def _quote_table_id(table_id: str) -> str:
    """Valida e prepara um identificador BigQuery project.dataset.table."""

    if not re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", table_id):
        raise ValueError("table_id deve estar no formato project.dataset.table")
    return table_id
