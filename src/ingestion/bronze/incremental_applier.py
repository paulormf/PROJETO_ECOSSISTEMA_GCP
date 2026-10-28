"""Aplica eventos incrementais nas tabelas de negócio da camada Bronze."""

from dataclasses import dataclass
import re

from google.cloud import bigquery

from ingestion.bronze.policy import get_bronze_policy


_IDENTIFIER = re.compile(r"^[A-Za-z0-9_-]+$")
_FIELD_TYPE_ALIASES = {
    "INTEGER": "INT64",
    "FLOAT": "FLOAT64",
    "BOOLEAN": "BOOL",
}
_METADATA_COLUMNS = {
    "source_system",
    "load_type",
    "source_schema",
    "source_table",
    "run_id",
    "snapshot_at",
    "ingested_at",
}


@dataclass(frozen=True, slots=True)
class TableApplyResult:
    """Resumo da aplicação incremental de uma tabela."""

    source_table: str
    rows_affected: int
    job_id: str


@dataclass(frozen=True, slots=True)
class IncrementalApplyResult:
    """Resumo da aplicação incremental de um run completo."""

    run_id: str
    tables_processed: int
    rows_affected: int
    tables: tuple[TableApplyResult, ...]


def apply_incremental_run(
    client: bigquery.Client,
    *,
    run_id: str,
    project_id: str,
    environment: str,
    source_schema: str = "ecommerce",
    location: str = "us-central1",
) -> IncrementalApplyResult:
    """Aplica um run técnico nas tabelas Bronze correspondentes."""

    _validate_identifier(project_id, "project_id")
    _validate_identifier(environment, "environment")
    _validate_identifier(source_schema, "source_schema")
    if not run_id.strip():
        raise ValueError("run_id não pode ser vazio")

    events_table = f"{project_id}.{environment}_bronze.bronze_incremental_events"
    table_names = _find_run_tables(
        client,
        events_table=events_table,
        source_schema=source_schema,
        run_id=run_id,
        location=location,
    )

    results: list[TableApplyResult] = []
    for source_table in table_names:
        destination_table = f"{project_id}.{environment}_bronze.{source_table}"
        table = client.get_table(destination_table)
        query = _build_merge_query(
            events_table=events_table,
            destination_table=destination_table,
            source_schema=source_schema,
            source_table=source_table,
            run_id=run_id,
            destination_schema=table.schema,
        )
        job = client.query(
            query,
            job_config=_query_config(source_schema, source_table, run_id),
            location=location,
        )
        job.result()
        results.append(
            TableApplyResult(
                source_table=source_table,
                rows_affected=int(job.num_dml_affected_rows or 0),
                job_id=job.job_id,
            )
        )

    return IncrementalApplyResult(
        run_id=run_id,
        tables_processed=len(results),
        rows_affected=sum(result.rows_affected for result in results),
        tables=tuple(results),
    )


def _find_run_tables(
    client: bigquery.Client,
    *,
    events_table: str,
    source_schema: str,
    run_id: str,
    location: str,
) -> tuple[str, ...]:
    query = f"""
    SELECT DISTINCT source_table
    FROM `{events_table}`
    WHERE source_schema = @source_schema
      AND run_id = @run_id
    ORDER BY source_table
    """
    job = client.query(
        query,
        job_config=_query_config(source_schema, None, run_id),
        location=location,
    )
    return tuple(row["source_table"] for row in job.result())


def _build_merge_query(
    *,
    events_table: str,
    destination_table: str,
    source_schema: str,
    source_table: str,
    run_id: str,
    destination_schema: list[bigquery.SchemaField],
) -> str:
    policy = get_bronze_policy(source_table)
    columns = {
        field.name: field
        for field in destination_schema
        if field.name not in _METADATA_COLUMNS
    }
    required_metadata = _METADATA_COLUMNS - {
        field.name for field in destination_schema
    }
    if required_metadata:
        missing = ", ".join(sorted(required_metadata))
        raise ValueError(
            f"Tabela {destination_table} não possui metadados Bronze: {missing}"
        )
    for key in policy.primary_key_fields:
        if key not in columns:
            raise ValueError(f"Chave {key} não existe em {destination_table}")

    match_conditions = [
        f"{_cast_json_value('source.primary_key', key, columns[key])} = "
        f"target.`{key}`"
        for key in policy.primary_key_fields
    ]
    update_assignments = [
        f"`{column}` = {_cast_json_value('source.new_row', column, field)}"
        for column, field in columns.items()
    ]
    update_assignments.extend(
        [
            "`source_system` = source.source_system",
            "`load_type` = source.load_type",
            "`source_schema` = source.source_schema",
            "`source_table` = source.source_table",
            "`run_id` = source.run_id",
            "`ingested_at` = source.ingested_at",
        ]
    )
    insert_columns = list(columns) + [
        "source_system",
        "load_type",
        "source_schema",
        "source_table",
        "run_id",
        "snapshot_at",
        "ingested_at",
    ]
    insert_values = [
        _cast_json_value("source.new_row", column, field)
        for column, field in columns.items()
    ] + [
        "source.source_system",
        "source.load_type",
        "source.source_schema",
        "source.source_table",
        "source.run_id",
        "CAST(NULL AS TIMESTAMP)",
        "source.ingested_at",
    ]

    return f"""
    MERGE `{destination_table}` AS target
    USING (
        SELECT *
        FROM `{events_table}`
        WHERE source_schema = @source_schema
          AND source_table = @source_table
          AND run_id = @run_id
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY primary_key
            ORDER BY change_id DESC
        ) = 1
    ) AS source
    ON {' AND '.join(match_conditions)}
    WHEN MATCHED AND source.operation = 'D' THEN DELETE
    WHEN MATCHED THEN UPDATE SET
        {', '.join(update_assignments)}
    WHEN NOT MATCHED AND source.operation <> 'D' THEN
        INSERT ({', '.join(f'`{column}`' for column in insert_columns)})
        VALUES ({', '.join(insert_values)})
    """


def _cast_json_value(
    json_column: str,
    column_name: str,
    field: bigquery.SchemaField,
) -> str:
    raw = f"JSON_VALUE({json_column}, '$.{column_name}')"
    field_type = _FIELD_TYPE_ALIASES.get(
        field.field_type.upper(),
        field.field_type.upper(),
    )
    if field_type == "STRING":
        return (
            f"COALESCE({raw}, JSON_QUERY({json_column}, '$.{column_name}'))"
        )
    if field_type == "JSON":
        return f"JSON_QUERY({json_column}, '$.{column_name}')"
    supported_types = {
        "BOOL",
        "DATE",
        "DATETIME",
        "FLOAT64",
        "INT64",
        "NUMERIC",
        "BIGNUMERIC",
        "TIME",
        "TIMESTAMP",
    }
    if field_type not in supported_types:
        raise ValueError(
            f"Tipo BigQuery não suportado para {column_name}: {field.field_type}"
        )
    return f"SAFE_CAST({raw} AS {field_type})"


def _query_config(
    source_schema: str,
    source_table: str | None,
    run_id: str,
) -> bigquery.QueryJobConfig:
    parameters = [
        bigquery.ScalarQueryParameter("source_schema", "STRING", source_schema),
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
    ]
    if source_table is not None:
        parameters.append(
            bigquery.ScalarQueryParameter("source_table", "STRING", source_table)
        )
    return bigquery.QueryJobConfig(query_parameters=parameters)


def _validate_identifier(value: str, name: str) -> None:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} contém caracteres inválidos")
