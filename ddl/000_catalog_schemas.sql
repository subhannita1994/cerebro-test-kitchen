-- =============================================================================
-- 000_catalog_schemas.sql — schemas + landing volume (ALL customers / variants)
-- -----------------------------------------------------------------------------
-- Schema-as-code, artifact #0. Idempotent + parameterized by the ${catalog}
-- placeholder token. The apply_ddl runner (src/pipelines/apply_ddl.py) reads
-- this file, substitutes ${catalog} with ${var.catalog} (e.g. cerebro_a), then
-- executes each statement via spark.sql().
--
-- Creates the three medallion schemas and the Auto Loader landing volume that
-- the seed job writes source files into. Safe to re-run.
-- =============================================================================

-- Pin the working catalog so unqualified names resolve here. The runner also
-- substitutes ${catalog} directly, so both patterns are demonstrated.
USE CATALOG ${catalog};

CREATE SCHEMA IF NOT EXISTS ${catalog}.bronze
  COMMENT 'Raw Auto Loader ingest of landed source files (+ _ingest_ts, _source_file).';

CREATE SCHEMA IF NOT EXISTS ${catalog}.silver
  COMMENT 'Cleaned, typed, de-duplicated streaming tables.';

CREATE SCHEMA IF NOT EXISTS ${catalog}.gold
  COMMENT 'Market analytics marts + UC functions. Genie reads this schema.';

-- Landing volume for Auto Loader source files. Layout written by the seed job:
--   /Volumes/${catalog}/gold/raw_landing/<source>/*.json
-- where <source> in {pos_sales, products, stores, promotions, online_orders}.
-- Streaming checkpoints also live under this volume (see common.py).
CREATE VOLUME IF NOT EXISTS ${catalog}.gold.raw_landing
  COMMENT 'Auto Loader source files + streaming checkpoints for the Cerebro pipelines.';
