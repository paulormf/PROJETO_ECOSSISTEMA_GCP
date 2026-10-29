-- Controle idempotente do full load por carga lógica e por arquivo.
-- Projeto: botbrewpub-ghsurp.
-- Localização dos datasets: us-central1.

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.dev_control.full_load_runs` (
  logical_load_id STRING NOT NULL,
  pipeline_name STRING NOT NULL,
  source_system STRING NOT NULL,
  source_schema STRING NOT NULL,
  source_table STRING NOT NULL,
  snapshot_at TIMESTAMP NOT NULL,
  status STRING NOT NULL,
  expected_rows INT64,
  expected_files INT64,
  extracted_rows INT64 NOT NULL,
  extracted_files INT64 NOT NULL,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  error_message STRING,
  updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(started_at)
CLUSTER BY source_system, source_schema, source_table, status;

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.dev_control.full_load_files` (
  logical_load_id STRING NOT NULL,
  attempt_id STRING NOT NULL,
  source_system STRING NOT NULL,
  source_schema STRING NOT NULL,
  source_table STRING NOT NULL,
  object_name STRING NOT NULL,
  row_count INT64 NOT NULL,
  status STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  loaded_at TIMESTAMP,
  error_message STRING
)
PARTITION BY DATE(created_at)
CLUSTER BY source_system, source_schema, source_table, logical_load_id;

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.hom_control.full_load_runs` (
  logical_load_id STRING NOT NULL,
  pipeline_name STRING NOT NULL,
  source_system STRING NOT NULL,
  source_schema STRING NOT NULL,
  source_table STRING NOT NULL,
  snapshot_at TIMESTAMP NOT NULL,
  status STRING NOT NULL,
  expected_rows INT64,
  expected_files INT64,
  extracted_rows INT64 NOT NULL,
  extracted_files INT64 NOT NULL,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  error_message STRING,
  updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(started_at)
CLUSTER BY source_system, source_schema, source_table, status;

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.hom_control.full_load_files` (
  logical_load_id STRING NOT NULL,
  attempt_id STRING NOT NULL,
  source_system STRING NOT NULL,
  source_schema STRING NOT NULL,
  source_table STRING NOT NULL,
  object_name STRING NOT NULL,
  row_count INT64 NOT NULL,
  status STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  loaded_at TIMESTAMP,
  error_message STRING
)
PARTITION BY DATE(created_at)
CLUSTER BY source_system, source_schema, source_table, logical_load_id;

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.prod_control.full_load_runs` (
  logical_load_id STRING NOT NULL,
  pipeline_name STRING NOT NULL,
  source_system STRING NOT NULL,
  source_schema STRING NOT NULL,
  source_table STRING NOT NULL,
  snapshot_at TIMESTAMP NOT NULL,
  status STRING NOT NULL,
  expected_rows INT64,
  expected_files INT64,
  extracted_rows INT64 NOT NULL,
  extracted_files INT64 NOT NULL,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  error_message STRING,
  updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(started_at)
CLUSTER BY source_system, source_schema, source_table, status;

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.prod_control.full_load_files` (
  logical_load_id STRING NOT NULL,
  attempt_id STRING NOT NULL,
  source_system STRING NOT NULL,
  source_schema STRING NOT NULL,
  source_table STRING NOT NULL,
  object_name STRING NOT NULL,
  row_count INT64 NOT NULL,
  status STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  loaded_at TIMESTAMP,
  error_message STRING
)
PARTITION BY DATE(created_at)
CLUSTER BY source_system, source_schema, source_table, logical_load_id;
