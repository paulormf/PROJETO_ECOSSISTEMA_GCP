-- Controle do full inicial por tabela de origem.
-- Projeto: botbrewpub-ghsurp.
-- Localização dos datasets: us-central1.

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.dev_control.full_load_checkpoints` (
  pipeline_name STRING NOT NULL,
  source_system STRING NOT NULL,
  source_schema STRING NOT NULL,
  source_table STRING NOT NULL,
  run_id STRING NOT NULL,
  snapshot_at TIMESTAMP,
  last_primary_key_json STRING,
  rows_extracted INT64 NOT NULL,
  files_created INT64 NOT NULL,
  status STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  error_message STRING,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.hom_control.full_load_checkpoints` (
  pipeline_name STRING NOT NULL,
  source_system STRING NOT NULL,
  source_schema STRING NOT NULL,
  source_table STRING NOT NULL,
  run_id STRING NOT NULL,
  snapshot_at TIMESTAMP,
  last_primary_key_json STRING,
  rows_extracted INT64 NOT NULL,
  files_created INT64 NOT NULL,
  status STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  error_message STRING,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.prod_control.full_load_checkpoints` (
  pipeline_name STRING NOT NULL,
  source_system STRING NOT NULL,
  source_schema STRING NOT NULL,
  source_table STRING NOT NULL,
  run_id STRING NOT NULL,
  snapshot_at TIMESTAMP,
  last_primary_key_json STRING,
  rows_extracted INT64 NOT NULL,
  files_created INT64 NOT NULL,
  status STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  error_message STRING,
  updated_at TIMESTAMP NOT NULL
);
