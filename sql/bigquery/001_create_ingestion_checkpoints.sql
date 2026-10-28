-- Controle de processamento da ingestão incremental.
-- Projeto único: botbrewpub-ghsurp.
-- No BigQuery, os ambientes serão separados por datasets.

CREATE SCHEMA IF NOT EXISTS `botbrewpub-ghsurp.dev_control`
OPTIONS (location = 'us-central1');

CREATE SCHEMA IF NOT EXISTS `botbrewpub-ghsurp.hom_control`
OPTIONS (location = 'us-central1');

CREATE SCHEMA IF NOT EXISTS `botbrewpub-ghsurp.prod_control`
OPTIONS (location = 'us-central1');

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.dev_control.ingestion_checkpoints` (
  pipeline_name STRING NOT NULL,
  source_system STRING NOT NULL,
  last_change_id INT64 NOT NULL,
  last_processed_at TIMESTAMP,
  last_run_id STRING,
  status STRING NOT NULL,
  error_message STRING,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.hom_control.ingestion_checkpoints` (
  pipeline_name STRING NOT NULL,
  source_system STRING NOT NULL,
  last_change_id INT64 NOT NULL,
  last_processed_at TIMESTAMP,
  last_run_id STRING,
  status STRING NOT NULL,
  error_message STRING,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.prod_control.ingestion_checkpoints` (
  pipeline_name STRING NOT NULL,
  source_system STRING NOT NULL,
  last_change_id INT64 NOT NULL,
  last_processed_at TIMESTAMP,
  last_run_id STRING,
  status STRING NOT NULL,
  error_message STRING,
  updated_at TIMESTAMP NOT NULL
);
