# =============================================================================
# common.py — shared helpers for all three streaming pipeline variants
# -----------------------------------------------------------------------------
# Used by baseline/, omnichannel/, and altlogic/ pipeline.py. Holds:
#   - source schemas (match CONTRACT.md § Source tables exactly),
#   - the Auto Loader (cloudFiles) reader,
#   - checkpoint/landing path helpers keyed by CATALOG (so the four customer
#     catalogs never collide), and
#   - a triggered (availableNow) writeStream helper so a workshop run TERMINATES.
#
# These are the ingest primitives; the per-variant transform logic (what makes
# baseline vs. omnichannel vs. altlogic different) lives in each pipeline.py and
# is clearly marked there.
# =============================================================================

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, TimestampType, DateType,
)

# -----------------------------------------------------------------------------
# Source schemas — declared explicitly (no inference) so bronze is typed and
# stable across the four catalogs. Names/types match CONTRACT.md exactly.
# -----------------------------------------------------------------------------
SOURCE_SCHEMAS = {
    "pos_sales": StructType([
        StructField("transaction_id", StringType()),
        StructField("store_id", StringType()),
        StructField("product_id", StringType()),
        StructField("qty", IntegerType()),
        StructField("unit_price", DecimalType(10, 2)),
        StructField("txn_ts", TimestampType()),
    ]),
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


# -----------------------------------------------------------------------------
# Path helpers — everything is keyed by CATALOG so cerebro_a/b/c/dev never mix.
# -----------------------------------------------------------------------------
def landing_path(catalog: str, source: str) -> str:
    """Where the seed job drops JSON files for a given source."""
    return f"/Volumes/{catalog}/gold/raw_landing/{source}"


def checkpoint_path(catalog: str, name: str) -> str:
    """Streaming checkpoint location under the landing volume, keyed by catalog +
    stream name so each stream (and each catalog) has an isolated checkpoint."""
    return f"/Volumes/{catalog}/gold/raw_landing/_checkpoints/{name}"


def schema_location(catalog: str, source: str) -> str:
    """Auto Loader schema-tracking location for a source (also under the volume)."""
    return f"/Volumes/{catalog}/gold/raw_landing/_schemas/{source}"


# -----------------------------------------------------------------------------
# Auto Loader reader — cloudFiles over JSON with an explicit schema.
# -----------------------------------------------------------------------------
def read_stream(spark: SparkSession, catalog: str, source: str) -> DataFrame:
    """readStream via Auto Loader for one landed source. Adds housekeeping cols
    (_ingest_ts, _source_file) and the rescued-data column for schema drift."""
    schema = SOURCE_SCHEMAS[source]
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location(catalog, source))
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .schema(schema)
        .load(landing_path(catalog, source))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# -----------------------------------------------------------------------------
# Triggered sinks — availableNow so the stream drains the backlog and STOPS
# (workshop-friendly; not a forever-running continuous stream).
# -----------------------------------------------------------------------------
def write_bronze(df: DataFrame, catalog: str, table: str) -> None:
    """Append raw rows to a bronze table, triggered availableNow."""
    (df.writeStream
       .format("delta")
       .outputMode("append")
       .option("checkpointLocation", checkpoint_path(catalog, f"bronze_{table}"))
       .option("mergeSchema", "true")
       .trigger(availableNow=True)
       .toTable(f"{catalog}.bronze.{table}"))


def wait_for_streams(spark: SparkSession) -> None:
    """Block until all availableNow streams in this session finish, so a job task
    completes only after the batch has fully drained."""
    for q in spark.streams.active:
        q.awaitTermination()


# -----------------------------------------------------------------------------
# Dimension refresh — small dims are read as batch snapshots (overwrite) rather
# than streamed; they are tiny and change rarely. Kept here so all variants share
# the exact same dimension logic.
# -----------------------------------------------------------------------------
def refresh_dim_from_bronze(spark: SparkSession, catalog: str, source: str,
                            cols: list[str]) -> None:
    """Batch-overwrite a silver dimension from its bronze raw table (dedup on the
    first column = natural key, keeping the latest by _ingest_ts)."""
    key = cols[0]
    src = spark.table(f"{catalog}.bronze.{source}_raw")
    w = F.row_number().over(
        Window.partitionBy(key).orderBy(F.col("_ingest_ts").desc())
    )
    deduped = (src.withColumn("_rn", w).where(F.col("_rn") == 1)
               .select(*cols))
    (deduped.write.mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{catalog}.silver.{source}"))
