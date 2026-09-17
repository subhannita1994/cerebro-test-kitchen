-- =============================================================================
-- 001_bronze.sql — bronze raw-ingest tables (baseline: dev / A / C)
-- -----------------------------------------------------------------------------
-- One bronze table per landed source. Columns mirror the source contract with
-- Auto Loader housekeeping columns appended:
--   _ingest_ts    TIMESTAMP  — when this file's rows were ingested
--   _source_file  STRING     — the input_file_name() the row came from
--   _rescued_data STRING     — Auto Loader rescued-data column (schema drift)
--
-- Baseline set includes promotions (A/C). Customer B OMITS promotions and ADDS
-- online_orders via the overlay ddl/variants/customer_b/010_omnichannel.sql,
-- which the apply_ddl runner applies after these base files when
-- pipeline_variant == omnichannel.
--
-- Idempotent + parameterized by ${catalog}. Delta format.
-- =============================================================================

USE CATALOG ${catalog};

-- pos_sales: transaction_id, store_id, product_id, qty, unit_price, txn_ts
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.pos_sales_raw (
  transaction_id STRING,
  store_id       STRING,
  product_id     STRING,
  qty            INT,
  unit_price     DECIMAL(10,2),
  txn_ts         TIMESTAMP,
  _ingest_ts     TIMESTAMP,
  _source_file   STRING,
  _rescued_data  STRING
) USING DELTA
COMMENT 'Raw POS sales transactions landed via Auto Loader.';

-- products (dim): product_id, product_name, category, brand
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.products_raw (
  product_id    STRING,
  product_name  STRING,
  category      STRING,
  brand         STRING,
  _ingest_ts    TIMESTAMP,
  _source_file  STRING,
  _rescued_data STRING
) USING DELTA
COMMENT 'Raw product master (dimension) landed via Auto Loader.';

-- stores (dim): store_id, store_name, region, channel
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.stores_raw (
  store_id      STRING,
  store_name    STRING,
  region        STRING,
  channel       STRING,
  _ingest_ts    TIMESTAMP,
  _source_file  STRING,
  _rescued_data STRING
) USING DELTA
COMMENT 'Raw store master (dimension) landed via Auto Loader.';

-- promotions (A/C only): promo_id, product_id, discount_pct, start_date, end_date
-- Customer B never lands this source and its overlay does not create this table.
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.promotions_raw (
  promo_id      STRING,
  product_id    STRING,
  discount_pct  DECIMAL(5,2),
  start_date    DATE,
  end_date      DATE,
  _ingest_ts    TIMESTAMP,
  _source_file  STRING,
  _rescued_data STRING
) USING DELTA
COMMENT 'Raw promotions calendar landed via Auto Loader (Customers A/C only).';
