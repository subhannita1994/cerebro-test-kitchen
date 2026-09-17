-- =============================================================================
-- 003_gold.sql — gold analytics marts (baseline: dev / A / C)
-- -----------------------------------------------------------------------------
-- The marts Genie + the UC functions read. Column names/types match
-- CONTRACT.md § Gold EXACTLY so the AI swimlane's tools line up.
--
--   gold.sales_daily        daily units/revenue by product×store (+ dims)
--   gold.market_share       brand share of category revenue by region×period
--   gold.promo_performance  promo vs. baseline lift (A/C only)
--
-- Customer B OMITS promo_performance and ADDS omnichannel_sales via the overlay
-- (010_omnichannel.sql). For altlogic (C) the DDL is IDENTICAL to baseline —
-- only the pipeline transform for market_share.share_pct differs.
--
-- Idempotent + parameterized by ${catalog}. Delta format.
-- =============================================================================

USE CATALOG ${catalog};

CREATE TABLE IF NOT EXISTS ${catalog}.gold.sales_daily (
  sales_date DATE,
  product_id STRING,
  store_id   STRING,
  region     STRING,
  category   STRING,
  brand      STRING,
  units      BIGINT,
  revenue    DECIMAL(18,2)
) USING DELTA
COMMENT 'Daily units + revenue by product and store, enriched with store/product dims.';

CREATE TABLE IF NOT EXISTS ${catalog}.gold.market_share (
  period           STRING,
  region           STRING,
  category         STRING,
  brand            STRING,
  brand_revenue    DECIMAL(18,2),
  category_revenue DECIMAL(18,2),
  share_pct        DECIMAL(6,3)
) USING DELTA
COMMENT 'Brand share of category revenue by region and period. baseline/omnichannel = point-in-period; altlogic = trailing-4-week revenue-weighted.';

-- promo_performance (A/C only). Customer B's overlay does not create this table
-- and f_promo_lift is not deployed (enable_promo=false).
CREATE TABLE IF NOT EXISTS ${catalog}.gold.promo_performance (
  promo_id       STRING,
  product_id     STRING,
  baseline_units BIGINT,
  promo_units    BIGINT,
  lift_pct       DECIMAL(6,2)
) USING DELTA
COMMENT 'Promo vs. non-promo unit lift per promo/product (Customers A/C only).';
