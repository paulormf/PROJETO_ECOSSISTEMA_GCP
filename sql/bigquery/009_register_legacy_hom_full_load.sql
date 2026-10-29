-- Migração única do full load histórico de hom para o novo controle.
-- Não extrai dados e não altera os arquivos do GCS.
-- Execute na localização us-central1.

CREATE TEMP TABLE legacy_runs AS
SELECT *
FROM UNNEST([
  STRUCT('legacy-hom-20261029-armazens' AS logical_load_id, 'fbe33b01-a794-4f37-9e4b-7d023d6e8603' AS attempt_id, 'armazens' AS source_table, 5 AS total_rows, 1 AS file_count, 0 AS start_part, '[5]' AS last_key),
  STRUCT('legacy-hom-20261029-categorias', 'dc92d43a-8194-406e-b491-424d2e20285c', 'categorias', 100, 1, 0, '[100]'),
  STRUCT('legacy-hom-20261029-clientes', '6c406957-ad65-4c65-8c28-e027272fdbb9', 'clientes', 200000, 4, 0, '[200000]'),
  STRUCT('legacy-hom-20261029-enderecos-clientes', '71089aa2-9780-4e6d-a45b-1f4bac901d9a', 'enderecos_clientes', 200000, 4, 0, '[200000]'),
  STRUCT('legacy-hom-20261029-envios', 'aba5fbbc-c81b-4dfb-a96e-3b79697903cd', 'envios', 1000000, 20, 0, '[1000000]'),
  STRUCT('legacy-hom-20261029-estoque', '47df7661-236f-4768-bbdd-142d0b5ad649', 'estoque', 500000, 10, 0, '[5, 100000]'),
  STRUCT('legacy-hom-20261029-eventos-clientes', '052c8176-1590-4984-83f0-ce9497fcc856', 'eventos_clientes', 6250000, 125, 0, '[6250000]'),
  STRUCT('legacy-hom-20261029-eventos-clientes', '7828c49b-0809-4c14-b2c7-4536bb4c45ee', 'eventos_clientes', 3750000, 75, 125, '[10000000]'),
  STRUCT('legacy-hom-20261029-itens-pedido', 'e98d1ff0-3b1f-4afc-ac6a-f7d7d902a256', 'itens_pedido', 3000000, 60, 0, '[3000000]'),
  STRUCT('legacy-hom-20261029-marcas', 'd38c2006-5dfa-4c3f-bead-deafa9c54130', 'marcas', 500, 1, 0, '[500]'),
  STRUCT('legacy-hom-20261029-pagamentos', '767d061e-bdaf-448e-82a4-f5cfdaf558b1', 'pagamentos', 1000000, 20, 0, '[1000000]'),
  STRUCT('legacy-hom-20261029-pedidos', 'abb85740-4631-484b-832b-1fe134ec537c', 'pedidos', 1000000, 20, 0, '[1000000]'),
  STRUCT('legacy-hom-20261029-produtos', '6be2b57d-0b2e-4271-b619-66f3a10a08b9', 'produtos', 100000, 2, 0, '[100000]'),
  STRUCT('legacy-hom-20261029-variantes-produtos', 'b68ada1b-8116-4a27-9496-0234ff792027', 'variantes_produtos', 100000, 2, 0, '[100000]')
]);

MERGE `botbrewpub-ghsurp.hom_control.full_load_runs` AS target
USING (
  SELECT
    logical_load_id,
    'postgres-full-load' AS pipeline_name,
    'postgresql' AS source_system,
    'ecommerce' AS source_schema,
    ANY_VALUE(source_table) AS source_table,
    TIMESTAMP('2026-10-29 12:00:00+00') AS snapshot_at,
    'COMPLETED' AS status,
    SUM(total_rows) AS expected_rows,
    SUM(file_count) AS expected_files,
    SUM(total_rows) AS extracted_rows,
    SUM(file_count) AS extracted_files,
    TIMESTAMP('2026-10-29 12:00:00+00') AS started_at,
    TIMESTAMP('2026-10-29 13:00:00+00') AS completed_at,
    CAST(NULL AS STRING) AS error_message,
    TIMESTAMP('2026-10-29 13:00:00+00') AS updated_at,
    ARRAY_AGG(last_key ORDER BY start_part DESC LIMIT 1)[OFFSET(0)]
      AS last_primary_key_json
  FROM legacy_runs
  GROUP BY logical_load_id
) AS source
ON target.logical_load_id = source.logical_load_id
WHEN MATCHED THEN UPDATE SET
  status = source.status,
  expected_rows = source.expected_rows,
  expected_files = source.expected_files,
  extracted_rows = source.extracted_rows,
  extracted_files = source.extracted_files,
  completed_at = source.completed_at,
  error_message = source.error_message,
  updated_at = source.updated_at,
  last_primary_key_json = source.last_primary_key_json
WHEN NOT MATCHED THEN INSERT (
  logical_load_id, pipeline_name, source_system, source_schema, source_table,
  snapshot_at, status, expected_rows, expected_files, extracted_rows,
  extracted_files, started_at, completed_at, error_message, updated_at,
  last_primary_key_json
) VALUES (
  source.logical_load_id, source.pipeline_name, source.source_system,
  source.source_schema, source.source_table, source.snapshot_at, source.status,
  source.expected_rows, source.expected_files, source.extracted_rows,
  source.extracted_files, source.started_at, source.completed_at,
  source.error_message, source.updated_at, source.last_primary_key_json
);

MERGE `botbrewpub-ghsurp.hom_control.full_load_files` AS target
USING (
  SELECT
    logical_load_id,
    attempt_id,
    'postgresql' AS source_system,
    'ecommerce' AS source_schema,
    source_table,
    FORMAT(
      'source=postgresql/schema=ecommerce/table=%s/load_type=full/ingestion_date=2026-10-29/run_id=%s/part-%05d.parquet',
      source_table,
      attempt_id,
      part_number
    ) AS object_name,
    IF(
      part_number = start_part + file_count - 1,
      total_rows - 50000 * (file_count - 1),
      50000
    ) AS row_count,
    'LOADED' AS status,
    TIMESTAMP('2026-10-29 13:00:00+00') AS created_at,
    TIMESTAMP('2026-10-29 13:00:00+00') AS loaded_at,
    CAST(NULL AS STRING) AS error_message
  FROM legacy_runs
  CROSS JOIN UNNEST(
    GENERATE_ARRAY(start_part, start_part + file_count - 1)
  ) AS part_number
) AS source
ON target.logical_load_id = source.logical_load_id
AND target.object_name = source.object_name
WHEN MATCHED THEN UPDATE SET
  status = source.status,
  loaded_at = source.loaded_at,
  error_message = source.error_message
WHEN NOT MATCHED THEN INSERT (
  logical_load_id, attempt_id, source_system, source_schema, source_table,
  object_name, row_count, status, created_at, loaded_at, error_message
) VALUES (
  source.logical_load_id, source.attempt_id, source.source_system,
  source.source_schema, source.source_table, source.object_name,
  source.row_count, source.status, source.created_at, source.loaded_at,
  source.error_message
);
