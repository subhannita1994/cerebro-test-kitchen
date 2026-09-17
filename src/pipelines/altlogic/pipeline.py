# Databricks notebook source
# =============================================================================
# altlogic/pipeline.py — ALT-LOGIC streaming pipeline (Customer C)
# -----------------------------------------------------------------------------
# pipeline_variant = "altlogic". This is the CODE-VARIATION variant:
# the SCHEMA is IDENTICAL to baseline (same DDL, includes promotions), only the
# TRANSFORM logic differs. bronze -> silver -> gold, Structured Streaming +
# batch gold, TRIGGERED (availableNow).
#
# EXACTLY TWO things differ from baseline/pipeline.py — everything else
# (bronze ingest incl. promotions, silver dims, gold.sales_daily,
# gold.promo_performance) is intentionally IDENTICAL:
#
#   (A) WATERMARK is WIDER — 7 days instead of 1 day — so silver.pos_sales dedup
#       tolerates a much longer late-arrival window.
#
#   (B) gold.market_share.share_pct uses a TRAILING-4-WEEK REVENUE-WEIGHTED
#       methodology: for each week (period), brand_revenue/category_revenue are
#       summed over that week PLUS the 3 preceding weeks, and share is the ratio
#       of those trailing sums. (A revenue-weighted average of the weekly shares
#       equals this trailing-sum ratio — higher-revenue weeks dominate.) Baseline
#       instead uses point-in-period (single-week) share.
#
# This is the "code variation, not schema variation" teaching point: same tables,
# same columns, same UC functions — different numbers because the pipeline math
# differs.
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


# ---- (A) DIFFERS FROM BASELINE: wider watermark / dedup window. --------------
WATERMARK = "7 days"        # baseline uses "1 day"


def get_catalog(spark: SparkSession) -> str:
    try:
        dbutils  # type: ignore  # noqa: F821
        dbutils.widgets.text("catalog", "cerebro_c", "Target catalog")  # type: ignore # noqa: F821
        return dbutils.widgets.get("catalog")  # type: ignore  # noqa: F821
    except NameError:
        import argparse
        p = argparse.ArgumentParser()
        p.add_argument("--catalog", default="cerebro_c")
        return p.parse_args().catalog


# -----------------------------------------------------------------------------
# 1) BRONZE — IDENTICAL to baseline (includes promotions).
# -----------------------------------------------------------------------------
def ingest_bronze(spark: SparkSession, catalog: str) -> None:
    for source, table in [
        ("pos_sales", "pos_sales_raw"),
        ("products", "products_raw"),
        ("stores", "stores_raw"),
        ("promotions", "promotions_raw"),
    ]:
        df = common.read_stream(spark, catalog, source)
        common.write_bronze(df, catalog, table)
    common.wait_for_streams(spark)
    print("[altlogic] bronze ingest complete")


# -----------------------------------------------------------------------------
# 2) SILVER — same as baseline EXCEPT the wider WATERMARK on the fact dedup.
# -----------------------------------------------------------------------------
def build_silver(spark: SparkSession, catalog: str) -> None:
    common.refresh_dim_from_bronze(spark, catalog, "products",
                                   ["product_id", "product_name", "category", "brand"])
    common.refresh_dim_from_bronze(spark, catalog, "stores",
                                   ["store_id", "store_name", "region", "channel"])
    common.refresh_dim_from_bronze(spark, catalog, "promotions",
                                   ["promo_id", "product_id", "discount_pct",
                                    "start_date", "end_date"])

    fact = (
        spark.readStream.table(f"{catalog}.bronze.pos_sales_raw")
        .where(F.col("transaction_id").isNotNull() & F.col("txn_ts").isNotNull())
        .withWatermark("txn_ts", WATERMARK)                    # <-- (A) WIDER window
        .dropDuplicatesWithinWatermark(["transaction_id"])
        .select("transaction_id", "store_id", "product_id", "qty", "unit_price", "txn_ts")
    )
    (fact.writeStream.format("delta").outputMode("append")
         .option("checkpointLocation", common.checkpoint_path(catalog, "silver_pos_sales"))
         .trigger(availableNow=True)
         .toTable(f"{catalog}.silver.pos_sales"))
    common.wait_for_streams(spark)
    print("[altlogic] silver build complete")


# -----------------------------------------------------------------------------
# 3) GOLD — sales_daily + promo_performance IDENTICAL to baseline; market_share
#           uses the (B) trailing-4-week revenue-weighted methodology.
# -----------------------------------------------------------------------------
def build_gold(spark: SparkSession, catalog: str) -> None:
    spark.sql(f"USE CATALOG {catalog}")

    # --- gold.sales_daily: IDENTICAL to baseline. ---
    spark.sql(f"""
        INSERT OVERWRITE {catalog}.gold.sales_daily
        SELECT
          CAST(s.txn_ts AS DATE)                          AS sales_date,
          s.product_id, s.store_id, st.region, p.category, p.brand,
          CAST(SUM(s.qty) AS BIGINT)                      AS units,
          CAST(SUM(s.qty * s.unit_price) AS DECIMAL(18,2)) AS revenue
        FROM {catalog}.silver.pos_sales s
        JOIN {catalog}.silver.products p ON s.product_id = p.product_id
        JOIN {catalog}.silver.stores  st ON s.store_id   = st.store_id
        GROUP BY CAST(s.txn_ts AS DATE), s.product_id, s.store_id,
                 st.region, p.category, p.brand
    """)

    # --- gold.market_share: (B) TRAILING-4-WEEK REVENUE-WEIGHTED share. ---------
    # weekly     : brand revenue per Monday-anchored week.
    # week_idx   : integer week number (weeks since 1970-01-05, a Monday) so the
    #              RANGE window counts CALENDAR weeks and tolerates gaps.
    # t4_*       : trailing sum over [week-3, week] = this week + 3 prior weeks.
    # share      : trailing brand rev / trailing category rev (revenue-weighted).
    spark.sql(f"""
        INSERT OVERWRITE {catalog}.gold.market_share
        WITH weekly AS (
          SELECT
            CAST(date_trunc('WEEK', sales_date) AS DATE) AS week_start,
            region, category, brand,
            SUM(revenue) AS brand_revenue
          FROM {catalog}.gold.sales_daily
          GROUP BY 1, region, category, brand
        ),
        weekly_idx AS (
          SELECT *,
            CAST(datediff(week_start, DATE'1970-01-05') / 7 AS INT) AS week_idx
          FROM weekly
        ),
        trailing_brand AS (
          SELECT
            week_start, region, category, brand,
            SUM(brand_revenue) OVER (
              PARTITION BY region, category, brand
              ORDER BY week_idx
              RANGE BETWEEN 3 PRECEDING AND CURRENT ROW
            ) AS t4_brand_revenue
          FROM weekly_idx
        ),
        trailing_cat AS (
          SELECT week_start, region, category,
                 SUM(t4_brand_revenue) AS t4_category_revenue
          FROM trailing_brand
          GROUP BY week_start, region, category
        )
        SELECT
          concat(CAST(year(tb.week_start) AS STRING), '-W',
                 lpad(CAST(weekofyear(tb.week_start) AS STRING), 2, '0')) AS period,
          tb.region, tb.category, tb.brand,
          CAST(tb.t4_brand_revenue AS DECIMAL(18,2))    AS brand_revenue,
          CAST(tc.t4_category_revenue AS DECIMAL(18,2)) AS category_revenue,
          CAST(CASE WHEN tc.t4_category_revenue > 0
                    THEN 100.0 * tb.t4_brand_revenue / tc.t4_category_revenue
                    ELSE 0 END AS DECIMAL(6,3))         AS share_pct
        FROM trailing_brand tb
        JOIN trailing_cat tc
          ON tb.week_start = tc.week_start
         AND tb.region = tc.region AND tb.category = tc.category
    """)

    # --- gold.promo_performance: IDENTICAL to baseline (C keeps promotions). ---
    spark.sql(f"""
        INSERT OVERWRITE {catalog}.gold.promo_performance
        WITH daily AS (
          SELECT product_id, sales_date, SUM(units) AS units
          FROM {catalog}.gold.sales_daily
          GROUP BY product_id, sales_date
        ),
        promo_days AS (
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
        base_rate AS (
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
    print("[altlogic] gold build complete")


def main():
    spark = SparkSession.builder.getOrCreate()
    catalog = get_catalog(spark)
    print(f"[altlogic] variant=altlogic catalog={catalog}")
    ingest_bronze(spark, catalog)
    build_silver(spark, catalog)
    build_gold(spark, catalog)
    print("[altlogic] pipeline done.")


if __name__ == "__main__":
    main()
