"""Carregamento genérico de arquivos Parquet para tabelas Bronze."""

from dataclasses import dataclass
import re

from google.cloud import bigquery


_IDENTIFIER = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True, slots=True)
class LoadResult:
    """Resumo de um BigQuery Load Job."""

    job_id: str
    destination_table: str
    output_rows: int


def load_parquet_run(
    client: bigquery.Client,
    *,
    bucket_name: str,
    object_prefix: str,
    destination_project: str,
    destination_dataset: str,
    source_table: str,
    location: str = "us-central1",
    write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
    partition_field: str | None = None,
    clustering_fields: tuple[str, ...] = (),
) -> LoadResult:
    """Carrega todos os Parquets de um run na tabela de mesmo nome da origem.

    O particionamento e o clustering somente são aplicados quando a tabela
    destino é criada pelo Load Job. Alterar a configuração de uma tabela já
    existente exige uma etapa explícita de migração.
    """

    _validate_identifier(destination_project, "destination_project")
    _validate_identifier(destination_dataset, "destination_dataset")
    _validate_identifier(source_table, "source_table")
    if partition_field is not None:
        _validate_identifier(partition_field, "partition_field")
    for field in clustering_fields:
        _validate_identifier(field, "clustering_field")
    if not bucket_name.strip():
        raise ValueError("bucket_name não pode ser vazio")
    if not object_prefix.strip():
        raise ValueError("object_prefix não pode ser vazio")

    destination_table = (
        f"{destination_project}.{destination_dataset}.{source_table}"
    )
    source_uri = f"gs://{bucket_name}/{object_prefix.rstrip('/')}/part-*.parquet"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=write_disposition,
        autodetect=True,
        ignore_unknown_values=False,
    )
    if partition_field is not None:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
    if clustering_fields:
        job_config.clustering_fields = list(clustering_fields)
    load_job = client.load_table_from_uri(
        source_uri,
        destination_table,
        job_config=job_config,
        location=location,
    )
    load_job.result()

    return LoadResult(
        job_id=load_job.job_id,
        destination_table=destination_table,
        output_rows=int(load_job.output_rows or 0),
    )


def _validate_identifier(value: str, name: str) -> None:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} contém caracteres inválidos")
