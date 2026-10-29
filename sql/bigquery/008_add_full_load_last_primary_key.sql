-- Adiciona a posição de retomada da extração à carga full lógica.
-- Execute na localização us-central1.

ALTER TABLE `botbrewpub-ghsurp.dev_control.full_load_runs`
ADD COLUMN IF NOT EXISTS last_primary_key_json STRING;

ALTER TABLE `botbrewpub-ghsurp.hom_control.full_load_runs`
ADD COLUMN IF NOT EXISTS last_primary_key_json STRING;

ALTER TABLE `botbrewpub-ghsurp.prod_control.full_load_runs`
ADD COLUMN IF NOT EXISTS last_primary_key_json STRING;
