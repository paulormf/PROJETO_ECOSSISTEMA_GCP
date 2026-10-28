"""Ponto de entrada da ingestão incremental PostgreSQL -> GCS."""

import argparse
import json
import os
from dataclasses import asdict

from dotenv import load_dotenv
from google.cloud import bigquery, storage

from ingestion.control.checkpoint_store import BigQueryCheckpointStore
from ingestion.bronze.incremental_applier import apply_incremental_run
from ingestion.bronze.incremental_loader import load_incremental_run_to_events
from ingestion.incremental.change_log_reader import connect_read_only
from ingestion.incremental.runner import run_incremental


def main() -> int:
    """Executa uma janela da ingestão incremental e imprime o resumo."""

    load_dotenv()
    args = _parse_args()

    project_id = os.environ.get("GCP_PROJECT_ID", "botbrewpub-ghsurp")
    environment = args.environment
    location = os.environ.get("GCP_LOCATION", "us-central1")
    bucket_name = os.environ.get(
        "GCS_LANDING_BUCKET",
        f"{project_id}-{environment}-landing",
    )
    database_url = os.environ.get("SOURCE_DATABASE_URL", "")
    if not database_url:
        raise ValueError("SOURCE_DATABASE_URL não foi configurada")

    bq_client = bigquery.Client(project=project_id)
    storage_client = storage.Client(project=project_id)
    checkpoint_store = BigQueryCheckpointStore(
        bq_client,
        f"{project_id}.{environment}_control.ingestion_checkpoints",
    )
    previous_checkpoint = checkpoint_store.get(
        pipeline_name="postgres-incremental",
        source_system="postgresql",
    )
    previous_change_id = (
        previous_checkpoint.last_change_id if previous_checkpoint is not None else 0
    )

    with connect_read_only(database_url) as connection:
        result = run_incremental(
            connection,
            storage_client,
            checkpoint_store,
            bucket_name=bucket_name,
            page_size=args.page_size,
        )

    bronze_result = None
    bronze_apply_result = None
    if result.files_created > 0:
        try:
            bronze_result = load_incremental_run_to_events(
                storage_client,
                bq_client,
                bucket_name=bucket_name,
                run_id=result.run_id,
                destination_project=project_id,
                destination_dataset=f"{environment}_bronze",
                location=location,
            )
            bronze_apply_result = apply_incremental_run(
                bq_client,
                run_id=result.run_id,
                project_id=project_id,
                environment=environment,
                location=location,
            )
            if bronze_apply_result.tables_processed == 0:
                raise RuntimeError(
                    "Foram carregados eventos na Bronze técnica, mas nenhuma "
                    "tabela Bronze foi processada"
                )
        except Exception as error:
            checkpoint_store.save(
                pipeline_name="postgres-incremental",
                source_system="postgresql",
                last_change_id=previous_change_id,
                last_run_id=result.run_id,
                status="FAILED",
                error_message=(
                    "Falha no load da Bronze técnica: " + str(error)
                )[:4_000],
            )
            raise

    print(
        json.dumps(
            {
                "landing": asdict(result),
                "bronze_events": asdict(bronze_result)
                if bronze_result is not None
                else None,
                "bronze_apply": asdict(bronze_apply_result)
                if bronze_apply_result is not None
                else None,
            },
            ensure_ascii=False,
            default=str,
        )
    )
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa a ingestão incremental do PostgreSQL para o GCS."
    )
    parser.add_argument(
        "--environment",
        choices=("dev", "hom", "prod"),
        default=os.environ.get("GCP_ENVIRONMENT", "dev"),
        help="Ambiente de destino no GCP (padrão: dev).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=_read_positive_int("CHANGE_LOG_PAGE_SIZE", default=1_000),
        help="Quantidade máxima de eventos lidos por página.",
    )
    args = parser.parse_args()
    if args.page_size <= 0:
        parser.error("--page-size deve ser maior que zero")
    return args


def _read_positive_int(name: str, *, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} deve ser maior que zero")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
