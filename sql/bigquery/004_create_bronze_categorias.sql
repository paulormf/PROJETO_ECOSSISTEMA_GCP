-- Tabela Bronze do full inicial de categorias.
-- Projeto: botbrewpub-ghsurp.
-- Localização do dataset: us-central1.

CREATE TABLE IF NOT EXISTS `botbrewpub-ghsurp.dev_bronze.categorias_full` (
  categoria_id INT64 NOT NULL,
  categoria_pai_id INT64,
  nome_categoria STRING NOT NULL,
  criado_em TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  load_type STRING NOT NULL,
  source_schema STRING NOT NULL,
  source_table STRING NOT NULL,
  run_id STRING NOT NULL,
  snapshot_at TIMESTAMP,
  ingested_at TIMESTAMP NOT NULL
)
OPTIONS (
  description = 'Snapshot full inicial da tabela ecommerce.categorias',
  labels = [('layer', 'bronze'), ('source', 'postgresql'), ('load_type', 'full')]
);
