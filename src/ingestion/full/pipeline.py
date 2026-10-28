"""Orquestra o full inicial de tabelas selecionadas da origem."""

from collections.abc import Iterable
from dataclasses import dataclass

from google.cloud import bigquery
from google.cloud import storage
from psycopg import Connection

from ingestion.bronze.loader import LoadResult, load_parquet_run
from ingestion.bronze.policy import BronzeTablePolicy, get_bronze_policy
from ingestion.full.checkpoint_store import BigQueryFullLoadCheckpointStore
from ingestion.full.runner import FullLoadResult, run_full_table
from ingestion.source.catalog import discover_table_specs


@dataclass(frozen=True, slots=True)
class BronzeRunResult:
    """Resultado combinado da extração e da carga Bronze."""

    extraction: FullLoadResult
    load: LoadResult
    policy: BronzeTablePolicy


def run_selected_full_tables(
    connection: Connection,
    storage_client: storage.Client,
    checkpoint_store: BigQueryFullLoadCheckpointStore,
    *,
    bucket_name: str,
    source_schema: str,
    include_tables: Iterable[str],
    batch_size: int = 50_000,
) -> tuple[FullLoadResult, ...]:
    """Executa o full sequencialmente somente para tabelas selecionadas."""

    table_names = tuple(include_tables)
    if not table_names:
        raise ValueError("include_tables deve conter ao menos uma tabela")

    table_specs = discover_table_specs(
        connection,
        source_schema=source_schema,
        include_tables=table_names,
    )
    return tuple(
        run_full_table(
            connection,
            storage_client,
            checkpoint_store,
            table,
            bucket_name=bucket_name,
            batch_size=batch_size,
        )
        for table in table_specs
    )


def run_selected_full_tables_to_bronze(
    connection: Connection,
    storage_client: storage.Client,
    bigquery_client: bigquery.Client,
    checkpoint_store: BigQueryFullLoadCheckpointStore,
    *,
    bucket_name: str,
    source_schema: str,
    include_tables: Iterable[str],
    destination_project: str,
    destination_dataset: str,
    location: str = "us-central1",
    batch_size: int = 50_000,
) -> tuple[BronzeRunResult, ...]:
    """Executa full e Load Job sequencialmente com política Bronze."""

    table_names = tuple(include_tables)
    if not table_names:
        raise ValueError("include_tables deve conter ao menos uma tabela")

    table_specs = discover_table_specs(
        connection,
        source_schema=source_schema,
        include_tables=table_names,
    )
    policies = {table.source_table: get_bronze_policy(table.source_table) for table in table_specs}

    results: list[BronzeRunResult] = []
    for table in table_specs:
        extraction = run_full_table(
            connection,
            storage_client,
            checkpoint_store,
            table,
            bucket_name=bucket_name,
            batch_size=batch_size,
        )
        object_prefix = _find_run_prefix(
            storage_client,
            bucket_name=bucket_name,
            source_schema=source_schema,
            source_table=table.source_table,
            run_id=extraction.run_id,
        )
        policy = policies[table.source_table]
        load = load_parquet_run(
            bigquery_client,
            bucket_name=bucket_name,
            object_prefix=object_prefix,
            destination_project=destination_project,
            destination_dataset=destination_dataset,
            source_table=table.source_table,
            location=location,
            partition_field=policy.partition_field,
            clustering_fields=policy.clustering_fields,
        )
        results.append(
            BronzeRunResult(
                extraction=extraction,
                load=load,
                policy=policy,
            )
        )

    return tuple(results)


def _find_run_prefix(
    storage_client: storage.Client,
    *,
    bucket_name: str,
    source_schema: str,
    source_table: str,
    run_id: str,
) -> str:
    """Localiza o prefixo efetivamente criado para um run no GCS."""

    base_prefix = (
        f"source=postgresql/schema={source_schema}/"
        f"table={source_table}/load_type=full/"
    )
    run_marker = f"run_id={run_id}/"
    prefixes = {
        blob.name.rsplit("/part-", 1)[0]
        for blob in storage_client.list_blobs(bucket_name, prefix=base_prefix)
        if run_marker in blob.name
    }
    if len(prefixes) != 1:
        raise RuntimeError(
            f"Esperado um único prefixo para o run {run_id}, "
            f"encontrados: {sorted(prefixes)}"
        )
    return next(iter(prefixes))
