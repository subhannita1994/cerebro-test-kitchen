# Databricks notebook source
# =============================================================================
# generate_market_data.py — synthetic FGF bakery market data (ALL customers)
# -----------------------------------------------------------------------------
# Self-contained PySpark generator, runnable as a Databricks notebook OR a job
# task (params via dbutils.widgets when available, else argparse). Backs the
# `seed_data` DAB job.
#
# WHAT IT MAKES (FGF Brands = bakery/food manufacturer selling through retail):
#   - products   : ~60 baked goods across Breads/Bagels/Pastries/Croissants/Muffins
#   - stores     : ~40 stores across 5 regions and channels {Grocery,Club,Foodservice}
#   - pos_sales  : POS transactions w/ weekly seasonality + promo-driven spikes
#   - promotions : a handful of dated promos (only when include_promotions)
#   - online_orders : e-commerce orders (only when include_online_orders — Customer B)
#
# WHERE IT WRITES (two modes, controlled by `mode`):
#   - mode=files  (default) : JSON files into the UC volume
#         /Volumes/{catalog}/gold/raw_landing/<source>/  so Auto Loader ingests.
#   - mode=tables           : writes straight to bronze.<source>_raw Delta tables
#         (adds _ingest_ts/_source_file/_rescued_data) for a quick start with no
#         streaming.
#
# DETERMINISTIC: a fixed seed makes every run reproducible. Idempotent: each
# source's target is OVERWRITTEN, so re-running yields the same data (no dupes).
#
# Variant matrix (see CONTRACT.md):
#   dev / customer_a : include_promotions=true,  include_online_orders=false
#   customer_b       : include_promotions=false, include_online_orders=true
#   customer_c       : include_promotions=true,  include_online_orders=false
# =============================================================================

from __future__ import annotations

import argparse
import datetime as dt
import random
from decimal import Decimal, ROUND_HALF_UP

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, TimestampType, DateType,
)

# -----------------------------------------------------------------------------
# Reference dimensions — kept small + legible so the demo narrative is readable.
# -----------------------------------------------------------------------------
CATEGORIES = ["Breads", "Bagels", "Pastries", "Croissants", "Muffins"]
# A few FGF-style brands. Some brands span multiple categories (realistic).
BRANDS = ["Golden Hearth", "Sunrise Bakehouse", "Artisan Table", "Daily Crumb", "Stone Mill"]
REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
CHANNELS = ["Grocery", "Club", "Foodservice"]        # in-store channels
ONLINE_FULFILLMENT = ["Delivery", "Pickup"]           # for online_orders (Customer B)

SEED = 42  # deterministic


def _dec(x: float, places: str = "0.01") -> Decimal:
    """Round a float to a fixed-scale Decimal (matches DECIMAL(_,2) columns)."""
    return Decimal(str(x)).quantize(Decimal(places), rounding=ROUND_HALF_UP)


# -----------------------------------------------------------------------------
# Parameter plumbing — dbutils.widgets in a notebook, argparse on the CLI/job.
# -----------------------------------------------------------------------------
def _str2bool(v) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "y", "t")


def _resolve_online(include_online_orders, pipeline_variant: str) -> bool:
    """Online orders exist only for the omnichannel variant (Customer B). If the
    caller passed an explicit include_online_orders, honor it; otherwise derive
    it from pipeline_variant. This lets the seed_data job pass ONLY contract
    variables (catalog / enable_promo / pipeline_variant)."""
    if include_online_orders is not None and str(include_online_orders) != "":
        return _str2bool(include_online_orders)
    return pipeline_variant == "omnichannel"


def get_params():
    """Read params from dbutils.widgets if present, else argparse.

    Accepts pipeline_variant so the DAB job can pass contract variables directly;
    include_online_orders is derived from it when not given explicitly.
    """
    try:
        dbutils  # type: ignore  # noqa: F821 — provided in Databricks runtime
        dbutils.widgets.text("catalog", "cerebro_dev", "Target catalog")
        dbutils.widgets.dropdown("include_promotions", "true", ["true", "false"])
        dbutils.widgets.text("include_online_orders", "", "Override online orders (blank=derive)")
        dbutils.widgets.dropdown("pipeline_variant", "baseline",
                                 ["baseline", "omnichannel", "altlogic"])
        dbutils.widgets.text("days", "90", "Days of history")
        dbutils.widgets.text("num_stores", "40", "Number of stores")
        dbutils.widgets.text("num_products", "60", "Number of products")
        dbutils.widgets.dropdown("mode", "files", ["files", "tables"])
        g = dbutils.widgets.get  # type: ignore  # noqa: F821
        variant = g("pipeline_variant")
        return argparse.Namespace(
            catalog=g("catalog"),
            include_promotions=_str2bool(g("include_promotions")),
            include_online_orders=_resolve_online(g("include_online_orders"), variant),
            days=int(g("days")),
            num_stores=int(g("num_stores")),
            num_products=int(g("num_products")),
            mode=g("mode"),
        )
    except NameError:
        p = argparse.ArgumentParser(description="Generate FGF bakery market data.")
        p.add_argument("--catalog", default="cerebro_dev")
        p.add_argument("--include_promotions", default="true")
        p.add_argument("--include_online_orders", default="")
        p.add_argument("--pipeline_variant", default="baseline",
                       choices=["baseline", "omnichannel", "altlogic"])
        p.add_argument("--days", type=int, default=90)
        p.add_argument("--num_stores", type=int, default=40)
        p.add_argument("--num_products", type=int, default=60)
        p.add_argument("--mode", default="files", choices=["files", "tables"])
        a = p.parse_args()
        a.include_promotions = _str2bool(a.include_promotions)
        a.include_online_orders = _resolve_online(a.include_online_orders, a.pipeline_variant)
        return a


# -----------------------------------------------------------------------------
# Row builders — pure Python + a seeded RNG => reproducible.
# -----------------------------------------------------------------------------
def build_products(rng: random.Random, n: int):
    """n products spread across categories; each gets a brand + base price."""
    products = []
    for i in range(n):
        category = CATEGORIES[i % len(CATEGORIES)]
        brand = BRANDS[(i // len(CATEGORIES)) % len(BRANDS)]
        pid = f"P{i:04d}"
        name = f"{brand} {category[:-1] if category.endswith('s') else category} #{i:03d}"
        # Base price per category (dollars); a little per-product variation.
        base = {"Breads": 3.50, "Bagels": 4.25, "Pastries": 2.75,
                "Croissants": 3.10, "Muffins": 2.40}[category]
        price = _dec(base + rng.uniform(-0.40, 0.90))
        products.append({
            "product_id": pid, "product_name": name,
            "category": category, "brand": brand,
            "_base_price": price,  # helper (not written) for sales generation
        })
    return products


def build_stores(rng: random.Random, n: int):
    """n stores across regions/channels."""
    stores = []
    for i in range(n):
        region = REGIONS[i % len(REGIONS)]
        channel = CHANNELS[i % len(CHANNELS)]
        sid = f"S{i:04d}"
        stores.append({
            "store_id": sid,
            "store_name": f"{region} {channel} Store {i:03d}",
            "region": region, "channel": channel,
        })
    return stores


def build_promotions(rng: random.Random, products, start: dt.date, days: int):
    """A handful of dated promos on specific products. Returns (promos, promo_index)
    where promo_index maps date->{product_id: (promo_id, discount_pct)} for use in
    sales generation so promo windows show a demand SPIKE."""
    promos = []
    promo_index: dict[dt.date, dict[str, tuple]] = {}
    n_promos = max(4, days // 20)
    chosen = rng.sample(products, min(n_promos, len(products)))
    for j, prod in enumerate(chosen):
        # Promo window: 7-14 days somewhere inside the history.
        length = rng.randint(7, 14)
        offset = rng.randint(0, max(1, days - length - 1))
        p_start = start + dt.timedelta(days=offset)
        p_end = p_start + dt.timedelta(days=length - 1)
        discount = _dec(rng.choice([10.0, 15.0, 20.0, 25.0]), "0.01")
        promo_id = f"PROMO{j:03d}"
        promos.append({
            "promo_id": promo_id, "product_id": prod["product_id"],
            "discount_pct": discount, "start_date": p_start, "end_date": p_end,
        })
        d = p_start
        while d <= p_end:
            promo_index.setdefault(d, {})[prod["product_id"]] = (promo_id, float(discount))
            d += dt.timedelta(days=1)
    return promos, promo_index


def _weekly_multiplier(d: dt.date) -> float:
    """Weekly seasonality: bakeries sell more on Fri/Sat/Sun."""
    # Monday=0 .. Sunday=6
    return {0: 0.85, 1: 0.80, 2: 0.90, 3: 1.00, 4: 1.25, 5: 1.45, 6: 1.30}[d.weekday()]


def build_pos_sales(rng: random.Random, products, stores, start: dt.date, days: int,
                    promo_index: dict):
    """POS transactions with weekly seasonality + promo spikes. Each store sells a
    rotating subset of products each day."""
    rows = []
    tid = 0
    for day in range(days):
        d = start + dt.timedelta(days=day)
        season = _weekly_multiplier(d)
        for store in stores:
            # Each store carries ~15 products/day (subset rotates by day).
            k = min(len(products), 15)
            todays = rng.sample(products, k)
            for prod in todays:
                base_units = rng.randint(3, 20)
                units = int(round(base_units * season))
                # Promo spike: if this product is on promo today, boost demand and
                # apply the discount to unit_price.
                unit_price = prod["_base_price"]
                promo = promo_index.get(d, {}).get(prod["product_id"])
                if promo:
                    _promo_id, discount_pct = promo
                    units = int(round(units * rng.uniform(1.8, 2.6)))  # lift
                    unit_price = _dec(float(prod["_base_price"]) * (1 - discount_pct / 100.0))
                if units <= 0:
                    continue
                # A few transactions per product/store/day.
                n_txn = rng.randint(1, 3)
                for _ in range(n_txn):
                    q = max(1, units // n_txn)
                    hour = rng.randint(6, 20)
                    minute = rng.randint(0, 59)
                    txn_ts = dt.datetime(d.year, d.month, d.day, hour, minute,
                                         rng.randint(0, 59))
                    rows.append({
                        "transaction_id": f"T{tid:09d}",
                        "store_id": store["store_id"],
                        "product_id": prod["product_id"],
                        "qty": int(q),
                        "unit_price": unit_price,
                        "txn_ts": txn_ts,
                    })
                    tid += 1
    return rows


def build_online_orders(rng: random.Random, products, start: dt.date, days: int):
    """E-commerce orders for Customer B. Independent of physical stores; the
    omnichannel pipeline treats these as channel='Online'."""
    rows = []
    oid = 0
    for day in range(days):
        d = start + dt.timedelta(days=day)
        season = _weekly_multiplier(d)
        n_orders = int(round(rng.randint(40, 90) * season))
        for _ in range(n_orders):
            prod = rng.choice(products)
            q = rng.randint(1, 6)
            hour = rng.randint(0, 23)
            order_ts = dt.datetime(d.year, d.month, d.day, hour,
                                   rng.randint(0, 59), rng.randint(0, 59))
            rows.append({
                "order_id": f"O{oid:09d}",
                "product_id": prod["product_id"],
                "qty": int(q),
                "unit_price": prod["_base_price"],
                "order_ts": order_ts,
                "fulfillment": rng.choice(ONLINE_FULFILLMENT),
            })
            oid += 1
    return rows


# -----------------------------------------------------------------------------
# Spark schemas for each source (match CONTRACT.md § Source tables exactly).
# -----------------------------------------------------------------------------
SCHEMAS = {
    "products": StructType([
        StructField("product_id", StringType()),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("brand", StringType()),
    ]),
    "stores": StructType([
        StructField("store_id", StringType()),
        StructField("store_name", StringType()),
        StructField("region", StringType()),
        StructField("channel", StringType()),
    ]),
    "pos_sales": StructType([
        StructField("transaction_id", StringType()),
        StructField("store_id", StringType()),
        StructField("product_id", StringType()),
        StructField("qty", IntegerType()),
        StructField("unit_price", DecimalType(10, 2)),
        StructField("txn_ts", TimestampType()),
    ]),
    "promotions": StructType([
        StructField("promo_id", StringType()),
        StructField("product_id", StringType()),
        StructField("discount_pct", DecimalType(5, 2)),
        StructField("start_date", DateType()),
        StructField("end_date", DateType()),
    ]),
    "online_orders": StructType([
        StructField("order_id", StringType()),
        StructField("product_id", StringType()),
        StructField("qty", IntegerType()),
        StructField("unit_price", DecimalType(10, 2)),
        StructField("order_ts", TimestampType()),
        StructField("fulfillment", StringType()),
    ]),
}


def _project(rows, schema: StructType):
    """Drop helper keys (e.g. _base_price) so DataFrame matches the schema."""
    fields = [f.name for f in schema.fields]
    return [{k: r[k] for k in fields} for r in rows]


def write_source(spark, args, name: str, rows, schema: StructType):
    """Write one source either as JSON files (Auto Loader) or to a bronze table."""
    if not rows:
        print(f"[seed] {name}: 0 rows — skipped")
        return
    df = spark.createDataFrame(_project(rows, schema), schema=schema)

    if args.mode == "files":
        path = f"/Volumes/{args.catalog}/gold/raw_landing/{name}"
        # coalesce(1) => one tidy JSON file per source. overwrite => idempotent.
        (df.coalesce(1).write.mode("overwrite").format("json").save(path))
        print(f"[seed] {name}: wrote {df.count()} rows -> {path} (json)")
    else:  # mode == "tables" — write straight to bronze with housekeeping cols.
        from pyspark.sql import functions as F
        bronze = (df
                  .withColumn("_ingest_ts", F.current_timestamp())
                  .withColumn("_source_file", F.lit(f"seed://{name}"))
                  .withColumn("_rescued_data", F.lit(None).cast("string")))
        target = f"{args.catalog}.bronze.{name}_raw"
        (bronze.write.mode("overwrite").option("mergeSchema", "true")
               .saveAsTable(target))
        print(f"[seed] {name}: wrote {bronze.count()} rows -> {target} (delta)")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    args = get_params()
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG {args.catalog}")

    print(f"[seed] catalog={args.catalog} mode={args.mode} days={args.days} "
          f"stores={args.num_stores} products={args.num_products} "
          f"promo={args.include_promotions} online={args.include_online_orders}")

    rng = random.Random(SEED)
    start = dt.date.today() - dt.timedelta(days=args.days)

    products = build_products(rng, args.num_products)
    stores = build_stores(rng, args.num_stores)

    promotions, promo_index = ([], {})
    if args.include_promotions:
        promotions, promo_index = build_promotions(rng, products, start, args.days)

    pos_sales = build_pos_sales(rng, products, stores, start, args.days, promo_index)

    online_orders = []
    if args.include_online_orders:
        online_orders = build_online_orders(rng, products, start, args.days)

    # Always-present sources.
    write_source(spark, args, "products", products, SCHEMAS["products"])
    write_source(spark, args, "stores", stores, SCHEMAS["stores"])
    write_source(spark, args, "pos_sales", pos_sales, SCHEMAS["pos_sales"])

    # Variant-conditional sources.
    if args.include_promotions:
        write_source(spark, args, "promotions", promotions, SCHEMAS["promotions"])
    if args.include_online_orders:
        write_source(spark, args, "online_orders", online_orders, SCHEMAS["online_orders"])

    print("[seed] done.")


if __name__ == "__main__":
    main()
