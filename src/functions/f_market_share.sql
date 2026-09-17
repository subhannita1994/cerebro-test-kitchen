-- =============================================================================
-- f_market_share.sql — UC table function (ALL customers / variants)
-- -----------------------------------------------------------------------------
-- Backs the agent's `get_market_share` tool. Reads gold.market_share and
-- filters by category / region / period. Deployed for EVERY customer
-- (baseline, omnichannel, altlogic) — the underlying table shape is identical
-- in all variants, only the share_pct methodology differs (in the pipeline).
--
-- Parameterized by the literal ${catalog} token; the register-functions runner
-- (src/functions/register_functions.py) substitutes ${var.catalog} and executes.
-- CREATE OR REPLACE => idempotent + re-runnable.
--
-- Args accept a sentinel to mean "no filter":
--   pass NULL or 'all' (case-insensitive) for p_category / p_region to skip that
--   filter; pass NULL or 'latest' for p_period to return the most recent period.
-- =============================================================================

USE CATALOG ${catalog};

CREATE OR REPLACE FUNCTION ${catalog}.gold.f_market_share(
  p_category STRING COMMENT 'Product category (e.g. Breads); NULL or "all" for every category.',
  p_region   STRING COMMENT 'Region (e.g. Northeast); NULL or "all" for every region.',
  p_period   STRING COMMENT 'Period key (e.g. 2026-W12); NULL or "latest" for the most recent period.'
)
RETURNS TABLE (
  period           STRING,
  region           STRING,
  category         STRING,
  brand            STRING,
  brand_revenue    DECIMAL(18,2),
  category_revenue DECIMAL(18,2),
  share_pct        DECIMAL(6,3)
)
COMMENT 'Brand share of category revenue by region and period. Filters are optional (NULL / "all" / "latest").'
RETURN
  SELECT
    period, region, category, brand,
    brand_revenue, category_revenue, share_pct
  FROM ${catalog}.gold.market_share
  WHERE (p_category IS NULL OR lower(p_category) = 'all' OR category = p_category)
    AND (p_region   IS NULL OR lower(p_region)   = 'all' OR region   = p_region)
    AND (
          p_period IS NULL
          OR lower(p_period) = 'latest'
          OR period = p_period
        )
    -- When "latest" (or no period) is requested, keep only the max period that
    -- survives the category/region filters above.
    AND (
          (p_period IS NOT NULL AND lower(p_period) <> 'latest')
          OR period = (
            SELECT max(period)
            FROM ${catalog}.gold.market_share ms
            WHERE (p_category IS NULL OR lower(p_category) = 'all' OR ms.category = p_category)
              AND (p_region   IS NULL OR lower(p_region)   = 'all' OR ms.region   = p_region)
          )
        )
  ORDER BY share_pct DESC;
