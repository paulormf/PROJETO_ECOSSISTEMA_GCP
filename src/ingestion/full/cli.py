"""Ponto de entrada do full da camada Bronze."""

import argparse
import json
import os

import psycopg
from dotenv import load_dotenv
from google.cloud import bigquery, storage

from ingestion.full.checkpoint_store import BigQueryFullLoadCheckpointStore
from ingestion.full.pipeline import run_selected_full_tables_to_bronze


def main() -> None:
    """Executa o full das tabelas explicitamente informadas."""

    args = _parse_args()
    load_dotenv()

    database_url = _required_env("SOURCE_DATABASE_URL")
    project_id = os.getenv("GCP_PROJECT_ID", "botbrewpub-ghsurp")
    location = os.getenv("GCP_LOCATION", "us-central1")
    source_schema = os.getenv("SOURCE_SCHEMA", "ecommerce")
    bucket_name = os.getenv(
        "GCS_LANDING_BUCKET",
        f"{project_id}-{args.environment}-landing",
    )
    control_dataset = f"{args.environment}_control"
    bronze_dataset = f"{args.environment}_bronze"

    bigquery_client = bigquery.Client(project=project_id, location=location)
    storage_client = storage.Client(project=project_id)
    checkpoint_store = BigQueryFullLoadCheckpointStore(
        bigquery_client,
        f"{project_id}.{control_dataset}.full_load_checkpoints",
    )

    with psycopg.connect(database_url) as connection:
        results = run_selected_full_tables_to_bronze(
            connection,
            storage_client,
            bigquery_client,
            checkpoint_store,
            bucket_name=bucket_name,
            source_schema=source_schema,
            include_tables=args.tables,
            destination_project=project_id,
            destination_dataset=bronze_dataset,
            location=location,
            batch_size=args.batch_size,
        )

    for result in results:
        print(
            json.dumps(
                {
                    "source_table": result.extraction.source_table,
                    "run_id": result.extraction.run_id,
                    "rows_extracted": result.extraction.rows_extracted,
                    "files_created": result.extraction.files_created,
                    "status": result.extraction.status,
                    "destination_table": result.load.destination_table,
                    "load_job_id": result.load.job_id,
                    "output_rows": result.load.output_rows,
                    "partition_field": result.policy.partition_field,
                    "clustering_fields": result.policy.clustering_fields,
                },
                ensure_ascii=False,
            )
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa o full selecionado da Bronze."
    )
    parser.add_argument(
        "--environment",
        choices=("dev", "hom", "prod"),
        default="dev",
        help="Ambiente de destino; padrão: dev.",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        required=True,
        help="Tabelas do schema de origem a processar.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50_000,
        help="Quantidade de registros por lote; padrão: 50000.",
    )
    return parser.parse_args()


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variável de ambiente obrigatória ausente: {name}")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
