-- =============================================================================
-- 002_silver.sql — silver cleaned/typed/dedup tables (baseline: dev / A / C)
-- -----------------------------------------------------------------------------
-- Silver holds conformed, de-duplicated, business-ready rows. The streaming
-- pipeline dedups on the natural key with a watermark and drops null keys.
-- Types match the source contract exactly (see CONTRACT.md § Silver).
--
-- Baseline set includes promotions (A/C). Customer B OMITS promotions and ADDS
-- online_orders via the overlay (010_omnichannel.sql).
--
-- Idempotent + parameterized by ${catalog}. Delta format.
-- =============================================================================

USE CATALOG ${catalog};

CREATE TABLE IF NOT EXISTS ${catalog}.silver.pos_sales (
  transaction_id STRING,
  store_id       STRING,
  product_id     STRING,
  qty            INT,
  unit_price     DECIMAL(10,2),
  txn_ts         TIMESTAMP
) USING DELTA
COMMENT 'Cleaned, de-duplicated POS sales transactions (natural key: transaction_id).';

CREATE TABLE IF NOT EXISTS ${catalog}.silver.products (
  product_id   STRING,
  product_name STRING,
  category     STRING,
  brand        STRING
) USING DELTA
COMMENT 'Conformed product dimension (natural key: product_id).';

CREATE TABLE IF NOT EXISTS ${catalog}.silver.stores (
  store_id   STRING,
  store_name STRING,
  region     STRING,
  channel    STRING
) USING DELTA
COMMENT 'Conformed store dimension (natural key: store_id).';

-- promotions (A/C only). Customer B's overlay does not create this table.
CREATE TABLE IF NOT EXISTS ${catalog}.silver.promotions (
  promo_id     STRING,
  product_id   STRING,
  discount_pct DECIMAL(5,2),
  start_date   DATE,
  end_date     DATE
) USING DELTA
COMMENT 'Cleaned promotions calendar (Customers A/C only; natural key: promo_id).';
