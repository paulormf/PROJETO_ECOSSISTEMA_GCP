-- Tabela técnica da Bronze para receber os eventos incrementais do PostgreSQL.
-- Projeto: botbrewpub-ghsurp.
-- Localização dos datasets: us-central1.
--
-- Os campos primary_key, old_row e new_row permanecem como STRING contendo
-- JSON porque o mesmo envelope recebe eventos de todas as tabelas de origem.

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.dev_bronze.bronze_incremental_events`
(
  source_system STRING NOT NULL,
  load_type STRING NOT NULL,
  change_id INT64 NOT NULL,
  occurred_at TIMESTAMP NOT NULL,
  operation STRING NOT NULL,
  source_schema STRING NOT NULL,
  source_table STRING NOT NULL,
  primary_key STRING NOT NULL,
  old_row STRING,
  new_row STRING,
  request_id STRING,
  run_id STRING NOT NULL,
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY source_table, change_id
OPTIONS (
  description = 'Eventos incrementais brutos do PostgreSQL antes da aplicação nas tabelas Bronze',
  labels = [('layer', 'bronze'), ('source', 'postgresql'), ('load_type', 'incremental')]
);

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.hom_bronze.bronze_incremental_events`
(
  source_system STRING NOT NULL,
  load_type STRING NOT NULL,
  change_id INT64 NOT NULL,
  occurred_at TIMESTAMP NOT NULL,
  operation STRING NOT NULL,
  source_schema STRING NOT NULL,
  source_table STRING NOT NULL,
  primary_key STRING NOT NULL,
  old_row STRING,
  new_row STRING,
  request_id STRING,
  run_id STRING NOT NULL,
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY source_table, change_id
OPTIONS (
  description = 'Eventos incrementais brutos do PostgreSQL antes da aplicação nas tabelas Bronze',
  labels = [('layer', 'bronze'), ('source', 'postgresql'), ('load_type', 'incremental')]
);

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.prod_bronze.bronze_incremental_events`
(
  source_system STRING NOT NULL,
  load_type STRING NOT NULL,
  change_id INT64 NOT NULL,
  occurred_at TIMESTAMP NOT NULL,
  operation STRING NOT NULL,
  source_schema STRING NOT NULL,
  source_table STRING NOT NULL,
  primary_key STRING NOT NULL,
  old_row STRING,
  new_row STRING,
  request_id STRING,
  run_id STRING NOT NULL,
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY source_table, change_id
OPTIONS (
  description = 'Eventos incrementais brutos do PostgreSQL antes da aplicação nas tabelas Bronze',
  labels = [('layer', 'bronze'), ('source', 'postgresql'), ('load_type', 'incremental')]
);
