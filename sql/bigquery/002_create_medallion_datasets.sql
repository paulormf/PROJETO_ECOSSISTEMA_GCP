-- Datasets da arquitetura medalhão.
-- Projeto: botbrewpub-ghsurp.
-- Localização: us-central1.

CREATE SCHEMA IF NOT EXISTS `botbrewpub-ghsurp.dev_bronze`
OPTIONS (location = 'us-central1');

CREATE SCHEMA IF NOT EXISTS `botbrewpub-ghsurp.dev_silver`
OPTIONS (location = 'us-central1');

CREATE SCHEMA IF NOT EXISTS `botbrewpub-ghsurp.dev_gold`
OPTIONS (location = 'us-central1');

CREATE SCHEMA IF NOT EXISTS `botbrewpub-ghsurp.hom_bronze`
OPTIONS (location = 'us-central1');

CREATE SCHEMA IF NOT EXISTS `botbrewpub-ghsurp.hom_silver`
OPTIONS (location = 'us-central1');

CREATE SCHEMA IF NOT EXISTS `botbrewpub-ghsurp.hom_gold`
OPTIONS (location = 'us-central1');

CREATE SCHEMA IF NOT EXISTS `botbrewpub-ghsurp.prod_bronze`
OPTIONS (location = 'us-central1');

CREATE SCHEMA IF NOT EXISTS `botbrewpub-ghsurp.prod_silver`
OPTIONS (location = 'us-central1');

CREATE SCHEMA IF NOT EXISTS `botbrewpub-ghsurp.prod_gold`
OPTIONS (location = 'us-central1');
