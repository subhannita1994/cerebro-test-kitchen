# Databricks notebook source
# =============================================================================
# baseline/pipeline.py — BASELINE streaming pipeline (dev / Customer A)
# -----------------------------------------------------------------------------
# pipeline_variant = "baseline". Full set INCLUDING promotions.
# bronze -> silver -> gold via Structured Streaming (Auto Loader cloudFiles) +
# batch gold aggregations. Runs TRIGGERED (availableNow) so a workshop run drains
# the backlog and STOPS.
#
# WHAT THIS VARIANT DOES (contrast with the other two):
#   * INCLUDES promotions + gold.promo_performance.
#   * gold.market_share.share_pct = POINT-IN-PERIOD share
#       (brand_revenue / category_revenue within each period, per region+category).
#   * silver.pos_sales dedup uses a STANDARD 1-day watermark.
#
# Customer C (altlogic) keeps this exact SCHEMA but changes the market_share
# METHODOLOGY + widens the watermark — see altlogic/pipeline.py.
# Customer B (omnichannel) changes the SCHEMA — see omnichannel/pipeline.py.
# =============================================================================

from __future__ import annotations

import os
import sys

# Make src/pipelines/ importable so `import common` works whether this file runs
# as a notebook_task (where __file__ may be undefined) or a python file task.
def _bootstrap_common_path():
    cands = []
    try:
        cands.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    except NameError:
        pass
    d = os.getcwd()
    for _ in range(10):
        if os.path.exists(os.path.join(d, "src", "pipelines", "common.py")):
            cands.append(os.path.join(d, "src", "pipelines"))
            break
        if os.path.exists(os.path.join(d, "common.py")):
            cands.append(d)
            break
        nd = os.path.dirname(d)
        if nd == d:
            break
        d = nd
    for c in cands:
        if c and c not in sys.path:
            sys.path.insert(0, c)


_bootstrap_common_path()
import common  # noqa: E402

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


# ---- WATERMARK: baseline uses a standard 1-day late-arrival window. ----------
WATERMARK = "1 day"


def get_catalog(spark: SparkSession) -> str:
    try:
        dbutils  # type: ignore  # noqa: F821
        dbutils.widgets.text("catalog", "cerebro_dev", "Target catalog")  # type: ignore # noqa: F821
        return dbutils.widgets.get("catalog")  # type: ignore  # noqa: F821
    except NameError:
        import argparse
        p = argparse.ArgumentParser()
        p.add_argument("--catalog", default="cerebro_dev")
        return p.parse_args().catalog


# -----------------------------------------------------------------------------
# 1) BRONZE — Auto Loader landing -> bronze.*_raw (triggered append).
# -----------------------------------------------------------------------------
def ingest_bronze(spark: SparkSession, catalog: str) -> None:
    for source, table in [
        ("pos_sales", "pos_sales_raw"),
        ("products", "products_raw"),
        ("stores", "stores_raw"),
        ("promotions", "promotions_raw"),   # baseline INCLUDES promotions
    ]:
        df = common.read_stream(spark, catalog, source)
        common.write_bronze(df, catalog, table)
    common.wait_for_streams(spark)
    print("[baseline] bronze ingest complete")


# -----------------------------------------------------------------------------
# 2) SILVER — clean/type/dedup. Dims via batch refresh; fact via streaming dedup.
# -----------------------------------------------------------------------------
def build_silver(spark: SparkSession, catalog: str) -> None:
    # Dimensions (small, batch overwrite, dedup on natural key).
    common.refresh_dim_from_bronze(spark, catalog, "products",
                                   ["product_id", "product_name", "category", "brand"])
    common.refresh_dim_from_bronze(spark, catalog, "stores",
                                   ["store_id", "store_name", "region", "channel"])
    common.refresh_dim_from_bronze(spark, catalog, "promotions",
                                   ["promo_id", "product_id", "discount_pct",
                                    "start_date", "end_date"])

    # Fact: streaming dedup on transaction_id within the watermark, drop null keys.
    fact = (
        spark.readStream.table(f"{catalog}.bronze.pos_sales_raw")
        .where(F.col("transaction_id").isNotNull() & F.col("txn_ts").isNotNull())
        .withWatermark("txn_ts", WATERMARK)                       # <-- baseline watermark
        .dropDuplicatesWithinWatermark(["transaction_id"])
        .select("transaction_id", "store_id", "product_id", "qty", "unit_price", "txn_ts")
    )
    (fact.writeStream
         .format("delta")
         .outputMode("append")
         .option("checkpointLocation", common.checkpoint_path(catalog, "silver_pos_sales"))
         .trigger(availableNow=True)
         .toTable(f"{catalog}.silver.pos_sales"))
    common.wait_for_streams(spark)
    print("[baseline] silver build complete")


# -----------------------------------------------------------------------------
# 3) GOLD — batch aggregations off silver. Idempotent (overwrite).
# -----------------------------------------------------------------------------
def build_gold(spark: SparkSession, catalog: str) -> None:
    spark.sql(f"USE CATALOG {catalog}")

    # --- gold.sales_daily: daily units/revenue by product x store (+ dims). ---
    spark.sql(f"""
        INSERT OVERWRITE {catalog}.gold.sales_daily
        SELECT
          CAST(s.txn_ts AS DATE)                          AS sales_date,
          s.product_id,
          s.store_id,
          st.region,
          p.category,
          p.brand,
          CAST(SUM(s.qty) AS BIGINT)                      AS units,
          CAST(SUM(s.qty * s.unit_price) AS DECIMAL(18,2)) AS revenue
        FROM {catalog}.silver.pos_sales s
        JOIN {catalog}.silver.products p ON s.product_id = p.product_id
        JOIN {catalog}.silver.stores  st ON s.store_id   = st.store_id
        GROUP BY CAST(s.txn_ts AS DATE), s.product_id, s.store_id,
                 st.region, p.category, p.brand
    """)

    # --- gold.market_share: POINT-IN-PERIOD brand share of category revenue. ---
    # period = ISO year-week (e.g. 2026-W12). Share = brand_rev / category_rev
    # WITHIN that period+region+category. This is the baseline methodology; C
    # (altlogic) replaces exactly this block with a trailing-4-week weighting.
    spark.sql(f"""
        INSERT OVERWRITE {catalog}.gold.market_share
        WITH base AS (
          SELECT
            concat(CAST(year(sales_date) AS STRING), '-W',
                   lpad(CAST(weekofyear(sales_date) AS STRING), 2, '0')) AS period,
            region, category, brand,
            SUM(revenue) AS brand_revenue
          FROM {catalog}.gold.sales_daily
          GROUP BY 1, region, category, brand
        ),
        cat AS (
          SELECT period, region, category, SUM(brand_revenue) AS category_revenue
          FROM base GROUP BY period, region, category
        )
        SELECT
          b.period, b.region, b.category, b.brand,
          CAST(b.brand_revenue AS DECIMAL(18,2))    AS brand_revenue,
          CAST(c.category_revenue AS DECIMAL(18,2)) AS category_revenue,
          CAST(CASE WHEN c.category_revenue > 0
                    THEN 100.0 * b.brand_revenue / c.category_revenue
                    ELSE 0 END AS DECIMAL(6,3))     AS share_pct
        FROM base b
        JOIN cat c
          ON b.period = c.period AND b.region = c.region AND b.category = c.category
    """)

    # --- gold.promo_performance: promo vs. baseline unit lift (A/C only). ---
    # promo_units  = units of the product during its promo window.
    # baseline_units = that product's avg daily NON-promo units, scaled to the
    #                  promo length (like-for-like). lift = (promo-base)/base.
    spark.sql(f"""
        INSERT OVERWRITE {catalog}.gold.promo_performance
        WITH daily AS (
          SELECT product_id, sales_date, SUM(units) AS units
          FROM {catalog}.gold.sales_daily
          GROUP BY product_id, sales_date
        ),
        promo_days AS (   -- product-days that fall inside ANY promo window
          SELECT pr.promo_id, pr.product_id, d.sales_date, d.units,
                 datediff(pr.end_date, pr.start_date) + 1 AS promo_len
          FROM {catalog}.silver.promotions pr
          JOIN daily d
            ON d.product_id = pr.product_id
           AND d.sales_date BETWEEN pr.start_date AND pr.end_date
        ),
        promo_agg AS (
          SELECT promo_id, product_id, MAX(promo_len) AS promo_len,
                 CAST(SUM(units) AS BIGINT) AS promo_units
          FROM promo_days GROUP BY promo_id, product_id
        ),
        base_rate AS (    -- avg daily units on NON-promo days, per product
          SELECT d.product_id, AVG(d.units) AS avg_daily_units
          FROM daily d
          LEFT ANTI JOIN promo_days pd
            ON pd.product_id = d.product_id AND pd.sales_date = d.sales_date
          GROUP BY d.product_id
        )
        SELECT
          pa.promo_id, pa.product_id,
          CAST(ROUND(COALESCE(br.avg_daily_units, 0) * pa.promo_len) AS BIGINT) AS baseline_units,
          pa.promo_units,
          CAST(CASE
                 WHEN COALESCE(br.avg_daily_units, 0) * pa.promo_len > 0
                 THEN 100.0 * (pa.promo_units - br.avg_daily_units * pa.promo_len)
                      / (br.avg_daily_units * pa.promo_len)
                 ELSE 0 END AS DECIMAL(6,2)) AS lift_pct
        FROM promo_agg pa
        LEFT JOIN base_rate br ON pa.product_id = br.product_id
    """)
    print("[baseline] gold build complete")


def main():
    spark = SparkSession.builder.getOrCreate()
    catalog = get_catalog(spark)
    print(f"[baseline] variant=baseline catalog={catalog}")
    ingest_bronze(spark, catalog)
    build_silver(spark, catalog)
    build_gold(spark, catalog)
    print("[baseline] pipeline done.")


if __name__ == "__main__":
    main()
