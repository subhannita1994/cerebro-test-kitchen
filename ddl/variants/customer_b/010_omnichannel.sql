-- =============================================================================
-- 010_omnichannel.sql — Customer B OVERLAY (schema-as-code DIVERGENCE exercise)
-- -----------------------------------------------------------------------------
-- THIS IS THE "SCHEMA-AS-CODE DIVERGENCE" EXERCISE.
--
-- Customers A/C share the baseline schema (000..003). Customer B's business is
-- OMNICHANNEL (in-store + e-commerce) and has NO promotions program, so its
-- schema DIVERGES from baseline. Rather than fork the whole DDL, we keep the
-- base numbered files and layer this ONE overlay on top. The apply_ddl runner
-- applies it ONLY when ${var.pipeline_variant} == omnichannel (Customer B), and
-- it is written to be idempotent + re-runnable like every other artifact.
--
-- What diverges vs. baseline:
--   (1) DROP the promo objects that were created by the base files, because
--       Customer B has no promotions data (enable_promo=false). Dropping keeps
--       the catalog clean so Genie/tools never see empty promo tables.
--         - silver.promotions
--         - bronze.promotions_raw
--         - gold.promo_performance
--       (f_promo_lift is simply never registered for B — see functions.yml.)
--   (2) ADD the online e-commerce source path:
--         - bronze.online_orders_raw   (raw Auto Loader ingest)
--         - silver.online_orders       (cleaned/typed/dedup)
--   (3) ADD the omnichannel gold mart:
--         - gold.omnichannel_sales     (units/revenue by product × channel/day)
--   (4) NOTE: gold.sales_daily and gold.market_share are UNCHANGED in shape, but
--       for Customer B their ROWS include the Online channel (the omnichannel
--       pipeline unions store POS + online orders before aggregating). No DDL
--       change is needed for that — it is a data/transform difference.
--
-- Column names/types for the ADDED tables match CONTRACT.md EXACTLY.
-- =============================================================================

USE CATALOG ${catalog};

-- (1) Remove promo objects that the base files created (B has no promo data). ---
DROP TABLE IF EXISTS ${catalog}.gold.promo_performance;
DROP TABLE IF EXISTS ${catalog}.silver.promotions;
DROP TABLE IF EXISTS ${catalog}.bronze.promotions_raw;

-- (2) Online e-commerce source: bronze raw ingest. -----------------------------
-- online_orders: order_id, product_id, qty, unit_price, order_ts, fulfillment
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.online_orders_raw (
  order_id      STRING,
  product_id    STRING,
  qty           INT,
  unit_price    DECIMAL(10,2),
  order_ts      TIMESTAMP,
  fulfillment   STRING,
  _ingest_ts    TIMESTAMP,
  _source_file  STRING,
  _rescued_data STRING
) USING DELTA
COMMENT 'Raw online e-commerce orders landed via Auto Loader (Customer B only).';

-- (2) Online e-commerce source: silver cleaned/typed/dedup. --------------------
CREATE TABLE IF NOT EXISTS ${catalog}.silver.online_orders (
  order_id    STRING,
  product_id  STRING,
  qty         INT,
  unit_price  DECIMAL(10,2),
  order_ts    TIMESTAMP,
  fulfillment STRING
) USING DELTA
COMMENT 'Cleaned online orders (natural key: order_id; Customer B only).';

-- (3) Omnichannel gold mart. ---------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.gold.omnichannel_sales (
  sales_date DATE,
  product_id STRING,
  channel    STRING,          -- Grocery | Club | Foodservice | Online
  units      BIGINT,
  revenue    DECIMAL(18,2)
) USING DELTA
COMMENT 'Daily units + revenue by product and channel, including the Online channel (Customer B only).';
