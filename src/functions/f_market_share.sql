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
-- Args accept a sentinel to mean "no filter": pass NULL or 'all'/'latest'
-- (case-insensitive) for any arg to skip that filter. Rows come back
-- newest-period-first, so the latest period is at the top.
-- =============================================================================

USE CATALOG ${catalog};

CREATE OR REPLACE FUNCTION ${catalog}.gold.f_market_share(
  p_category STRING COMMENT 'Product category (e.g. Breads); NULL or "all" for every category.',
  p_region   STRING COMMENT 'Region (e.g. Northeast); NULL or "all" for every region.',
  p_period   STRING COMMENT 'Period key (e.g. 2026-W12); NULL or "all"/"latest" for all periods (newest first).'
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
  -- IMPORTANT: a SQL UDF's parameters CANNOT be referenced inside a nested /
  -- correlated subquery in the body — Spark resolves them as columns and fails
  -- with UNRESOLVED_COLUMN. So every parameter reference stays at the TOP LEVEL of
  -- a single SELECT (same pattern as f_promo_lift). NULL / 'all' / 'latest' on a
  -- filter means "no filter"; results are ordered newest-period-first so the
  -- caller (Claude) sees the most recent period at the top.
  SELECT
    period, region, category, brand,
    brand_revenue, category_revenue, share_pct
  FROM ${catalog}.gold.market_share
  WHERE (p_category IS NULL OR lower(p_category) = 'all' OR category = p_category)
    AND (p_region   IS NULL OR lower(p_region)   = 'all' OR region   = p_region)
    AND (p_period   IS NULL OR lower(p_period) IN ('all', 'latest') OR period = p_period)
  ORDER BY period DESC, share_pct DESC;
