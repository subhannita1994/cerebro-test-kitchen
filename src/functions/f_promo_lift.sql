-- =============================================================================
-- f_promo_lift.sql — UC table function (Customers A / C ONLY)
-- -----------------------------------------------------------------------------
-- Backs the agent's `get_promo_lift` tool. Reads gold.promo_performance.
--
-- ONLY DEPLOYED WHEN enable_promo = true (Customers A and C, and dev).
-- Customer B has no promotions data (gold.promo_performance does not exist), so
-- the register-functions runner SKIPS this file and the `get_promo_lift` tool is
-- not offered to the model (see config/customer_b.yaml).
--
-- Parameterized by the literal ${catalog} token; the runner substitutes
-- ${var.catalog} and executes. CREATE OR REPLACE => idempotent + re-runnable.
--
-- Args accept a sentinel to mean "no filter": pass NULL or 'all' (case-
-- insensitive) for either argument to skip that filter.
-- =============================================================================

USE CATALOG ${catalog};

CREATE OR REPLACE FUNCTION ${catalog}.gold.f_promo_lift(
  p_product_id STRING COMMENT 'Product id to filter on; NULL or "all" for every product.',
  p_promo_id   STRING COMMENT 'Promo id to filter on; NULL or "all" for every promo.'
)
RETURNS TABLE (
  promo_id       STRING,
  product_id     STRING,
  baseline_units BIGINT,
  promo_units    BIGINT,
  lift_pct       DECIMAL(6,2)
)
COMMENT 'Promo vs. baseline unit lift per promo/product. Filters are optional (NULL / "all").'
RETURN
  SELECT
    promo_id, product_id, baseline_units, promo_units, lift_pct
  FROM ${catalog}.gold.promo_performance
  WHERE (p_product_id IS NULL OR lower(p_product_id) = 'all' OR product_id = p_product_id)
    AND (p_promo_id   IS NULL OR lower(p_promo_id)   = 'all' OR promo_id   = p_promo_id)
  ORDER BY lift_pct DESC;
