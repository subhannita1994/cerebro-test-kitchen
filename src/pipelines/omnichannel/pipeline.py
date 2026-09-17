# Databricks notebook source
# =============================================================================
# omnichannel/pipeline.py — OMNICHANNEL streaming pipeline (Customer B)
# -----------------------------------------------------------------------------
# pipeline_variant = "omnichannel". This is the SCHEMA-DIVERGENT variant.
# bronze -> silver -> gold via Structured Streaming (Auto Loader) + batch gold.
# Runs TRIGGERED (availableNow).
#
# WHAT THIS VARIANT DOES (contrast with baseline):
#   * NO promotions anywhere (no promotions source, no gold.promo_performance).
#     The customer_b DDL overlay already dropped those objects.
#   * ADDS an online e-commerce source: bronze.online_orders_raw ->
#     silver.online_orders.
#   * Builds gold.omnichannel_sales (units/revenue by product x CHANNEL x day),
#     where channel is the store channel for POS and 'Online' for e-commerce.
#   * gold.sales_daily + gold.market_share INCLUDE online sales: online rows are
#     folded in with a synthetic store_id='ONLINE' and region='Online', so the
#     Online channel shows up as its own region bucket in market share.
#   * market_share methodology = POINT-IN-PERIOD (same as baseline).
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

WATERMARK = "1 day"          # standard, like baseline
ONLINE_STORE_ID = "ONLINE"   # synthetic store for online rows in sales_daily
ONLINE_REGION = "Online"     # synthetic region so market_share includes online


def get_catalog(spark: SparkSession) -> str:
    try:
        dbutils  # type: ignore  # noqa: F821
        dbutils.widgets.text("catalog", "cerebro_b", "Target catalog")  # type: ignore # noqa: F821
        return dbutils.widgets.get("catalog")  # type: ignore  # noqa: F821
    except NameError:
        import argparse
        p = argparse.ArgumentParser()
        p.add_argument("--catalog", default="cerebro_b")
        return p.parse_args().catalog


# -----------------------------------------------------------------------------
# 1) BRONZE — pos_sales, dims, AND online_orders. NO promotions.
# -----------------------------------------------------------------------------
def ingest_bronze(spark: SparkSession, catalog: str) -> None:
    for source, table in [
        ("pos_sales", "pos_sales_raw"),
        ("products", "products_raw"),
        ("stores", "stores_raw"),
        ("online_orders", "online_orders_raw"),   # <-- omnichannel ADDS this
    ]:
        df = common.read_stream(spark, catalog, source)
        common.write_bronze(df, catalog, table)
    common.wait_for_streams(spark)
    print("[omnichannel] bronze ingest complete")


# -----------------------------------------------------------------------------
# 2) SILVER — dims + two facts (pos_sales, online_orders). No promotions.
# -----------------------------------------------------------------------------
def build_silver(spark: SparkSession, catalog: str) -> None:
    common.refresh_dim_from_bronze(spark, catalog, "products",
                                   ["product_id", "product_name", "category", "brand"])
    common.refresh_dim_from_bronze(spark, catalog, "stores",
                                   ["store_id", "store_name", "region", "channel"])

    # POS fact — streaming dedup.
    pos = (
        spark.readStream.table(f"{catalog}.bronze.pos_sales_raw")
        .where(F.col("transaction_id").isNotNull() & F.col("txn_ts").isNotNull())
        .withWatermark("txn_ts", WATERMARK)
        .dropDuplicatesWithinWatermark(["transaction_id"])
        .select("transaction_id", "store_id", "product_id", "qty", "unit_price", "txn_ts")
    )
    (pos.writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", common.checkpoint_path(catalog, "silver_pos_sales"))
        .trigger(availableNow=True)
        .toTable(f"{catalog}.silver.pos_sales"))

    # Online fact — streaming dedup on order_id.
    online = (
        spark.readStream.table(f"{catalog}.bronze.online_orders_raw")
        .where(F.col("order_id").isNotNull() & F.col("order_ts").isNotNull())
        .withWatermark("order_ts", WATERMARK)
        .dropDuplicatesWithinWatermark(["order_id"])
        .select("order_id", "product_id", "qty", "unit_price", "order_ts", "fulfillment")
    )
    (online.writeStream.format("delta").outputMode("append")
           .option("checkpointLocation", common.checkpoint_path(catalog, "silver_online_orders"))
           .trigger(availableNow=True)
           .toTable(f"{catalog}.silver.online_orders"))

    common.wait_for_streams(spark)
    print("[omnichannel] silver build complete")


# -----------------------------------------------------------------------------
# 3) GOLD — sales_daily (POS + Online), omnichannel_sales, market_share.
# -----------------------------------------------------------------------------
def build_gold(spark: SparkSession, catalog: str) -> None:
    spark.sql(f"USE CATALOG {catalog}")

    # --- gold.sales_daily: POS rows (real stores) UNION online rows (synthetic
    #     store/region) so downstream market share includes the Online channel. ---
    spark.sql(f"""
        INSERT OVERWRITE {catalog}.gold.sales_daily
        WITH pos AS (
          SELECT
            CAST(s.txn_ts AS DATE) AS sales_date, s.product_id, s.store_id,
            st.region, p.category, p.brand,
            SUM(s.qty) AS units, SUM(s.qty * s.unit_price) AS revenue
          FROM {catalog}.silver.pos_sales s
          JOIN {catalog}.silver.products p ON s.product_id = p.product_id
          JOIN {catalog}.silver.stores  st ON s.store_id   = st.store_id
          GROUP BY CAST(s.txn_ts AS DATE), s.product_id, s.store_id,
                   st.region, p.category, p.brand
        ),
        online AS (
          SELECT
            CAST(o.order_ts AS DATE) AS sales_date, o.product_id,
            '{ONLINE_STORE_ID}' AS store_id, '{ONLINE_REGION}' AS region,
            p.category, p.brand,
            SUM(o.qty) AS units, SUM(o.qty * o.unit_price) AS revenue
          FROM {catalog}.silver.online_orders o
          JOIN {catalog}.silver.products p ON o.product_id = p.product_id
          GROUP BY CAST(o.order_ts AS DATE), o.product_id, p.category, p.brand
        )
        SELECT sales_date, product_id, store_id, region, category, brand,
               CAST(units AS BIGINT) AS units,
               CAST(revenue AS DECIMAL(18,2)) AS revenue
        FROM (SELECT * FROM pos UNION ALL SELECT * FROM online)
    """)

    # --- gold.omnichannel_sales: units/revenue by product x CHANNEL x day. ---
    # In-store channel comes from the store dim; online is the 'Online' channel.
    spark.sql(f"""
        INSERT OVERWRITE {catalog}.gold.omnichannel_sales
        WITH pos AS (
          SELECT CAST(s.txn_ts AS DATE) AS sales_date, s.product_id,
                 st.channel AS channel,
                 SUM(s.qty) AS units, SUM(s.qty * s.unit_price) AS revenue
          FROM {catalog}.silver.pos_sales s
          JOIN {catalog}.silver.stores st ON s.store_id = st.store_id
          GROUP BY CAST(s.txn_ts AS DATE), s.product_id, st.channel
        ),
        online AS (
          SELECT CAST(o.order_ts AS DATE) AS sales_date, o.product_id,
                 'Online' AS channel,
                 SUM(o.qty) AS units, SUM(o.qty * o.unit_price) AS revenue
          FROM {catalog}.silver.online_orders o
          GROUP BY CAST(o.order_ts AS DATE), o.product_id
        )
        SELECT sales_date, product_id, channel,
               CAST(units AS BIGINT) AS units,
               CAST(revenue AS DECIMAL(18,2)) AS revenue
        FROM (SELECT * FROM pos UNION ALL SELECT * FROM online)
    """)

    # --- gold.market_share: POINT-IN-PERIOD share, now including Online region. ---
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
    print("[omnichannel] gold build complete (NO promo_performance for Customer B)")


def main():
    spark = SparkSession.builder.getOrCreate()
    catalog = get_catalog(spark)
    print(f"[omnichannel] variant=omnichannel catalog={catalog}")
    ingest_bronze(spark, catalog)
    build_silver(spark, catalog)
    build_gold(spark, catalog)
    print("[omnichannel] pipeline done.")


if __name__ == "__main__":
    main()
