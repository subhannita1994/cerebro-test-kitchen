# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase setup — chat memory (all customers) + Customer-C serving
# MAGIC
# MAGIC Provisions the app's Lakebase (managed Postgres) objects. **Run this AS THE
# MAGIC APP SERVICE PRINCIPAL** (the Postgres table owner) — either as a Job with
# MAGIC `run_as` = the app SP, or interactively from the app's context. This is the
# MAGIC same ownership model documented in `lakebase_migration_notebook.py`:
# MAGIC `databricks_superuser` is NOT a real PG superuser, so only the owning app SP
# MAGIC (or a Job running AS it) can run DDL on these tables.
# MAGIC
# MAGIC What it creates:
# MAGIC 1. **Chat-memory tables** (all customers): `users`, `threads`, `messages`,
# MAGIC    `agent_state` — reuses the exact DDL the app uses (`src/app/state.py`).
# MAGIC 2. **Customer C only** (`enable_lakebase_serving = true`):
# MAGIC    - OLTP **`watchlist(user_id, product_id, note, created_at)`** the chat writes to.
# MAGIC    - **Synced table `market_share_snapshot`** — reverse-ETL of Delta
# MAGIC      `${catalog}.gold.market_share` into Lakebase for sub-second serving.

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "cerebro_c", "Target catalog")
dbutils.widgets.text("lakebase_instance", "cerebro-lakebase", "Lakebase instance name")
dbutils.widgets.text("lakebase_host", "REPLACE_ME", "Lakebase read_write host")
dbutils.widgets.text("lakebase_database", "databricks_postgres", "Postgres database")
dbutils.widgets.dropdown("enable_lakebase_serving", "true", ["true", "false"], "Customer C serving")

CATALOG = dbutils.widgets.get("catalog")
INSTANCE = dbutils.widgets.get("lakebase_instance")
DB_HOST = dbutils.widgets.get("lakebase_host")
DB_NAME = dbutils.widgets.get("lakebase_database")
ENABLE_SERVING = dbutils.widgets.get("enable_lakebase_serving").lower() == "true"

# COMMAND ----------

import uuid
import psycopg
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
who = w.current_user.me().user_name
print("running as:", who, "(must be the app SP for DDL to succeed)")

# Owner credential: when run-as = app SP, this is the table owner's PG credential
# (no secrets embedded, no SET ROLE needed).
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()), instance_names=[INSTANCE])


def pg():
    return psycopg.connect(host=DB_HOST, dbname=DB_NAME, user=who,
                           password=cred.token, sslmode="require", autocommit=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Chat-memory tables (all customers)
# MAGIC Identical DDL to `src/app/state.init_schema()` so the app and setup never drift.

# COMMAND ----------

CHAT_DDL = """
CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    persona TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS threads (
    thread_id TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    title TEXT,
    created_at DOUBLE PRECISION,
    updated_at DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS messages (
    message_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT,
    steps_json TEXT,
    created_at DOUBLE PRECISION,
    genie_conversation_id TEXT
);
CREATE TABLE IF NOT EXISTS agent_state (
    thread_id TEXT PRIMARY KEY,
    summary TEXT,
    last_genie_conversation_id TEXT,
    updated_at DOUBLE PRECISION
);
"""

with pg() as conn, conn.cursor() as cur:
    cur.execute(CHAT_DDL)
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY 1")
    print("public tables:", [r[0] for r in cur.fetchall()])
print("Chat-memory tables ready (owned by the app SP).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Customer-C OLTP watchlist
# MAGIC The chat's `save_to_watchlist` tool INSERTs here (as the app SP owner).

# COMMAND ----------

WATCHLIST_DDL = """
CREATE TABLE IF NOT EXISTS watchlist (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

if ENABLE_SERVING:
    with pg() as conn, conn.cursor() as cur:
        cur.execute(WATCHLIST_DDL)
    print("watchlist table ready.")
else:
    print("enable_lakebase_serving=false — skipping watchlist (not a Customer-C target).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Customer-C synced table `market_share_snapshot`
# MAGIC
# MAGIC A **synced table** continuously (or on a snapshot schedule) copies Delta
# MAGIC `${catalog}.gold.market_share` into Lakebase Postgres for OLTP-latency reads.
# MAGIC The app's `get_top_movers` tool reads it via psycopg (`state.top_movers`) —
# MAGIC no SQL warehouse spin-up, sub-second. This is the Databricks reverse-ETL /
# MAGIC synced-table pattern (see the `databricks-lakebase-provisioned` guidance).
# MAGIC
# MAGIC **Primary key** = the natural key of the market_share grain:
# MAGIC `(period, region, category, brand)`. **Columns** mirror `gold.market_share`.
# MAGIC
# MAGIC > The declarative equivalent lives in `resources/lakebase.yml` and is only
# MAGIC > deployed for targets where `enable_lakebase_serving = true`. The SDK call
# MAGIC > below is the imperative equivalent for running this notebook standalone.
# MAGIC > Field names on the synced-table API are still settling across SDK
# MAGIC > versions — **verify against the workspace's installed `databricks-sdk`**
# MAGIC > before relying on it; the SQL fallback in the next cell always works.

# COMMAND ----------

# Imperative (SDK) creation of the synced table. Idempotent-ish: catches "exists".
if ENABLE_SERVING:
    try:
        from databricks.sdk.service.database import (
            SyncedDatabaseTable, SyncedTableSpec, NewPipelineSpec,
            SyncedTableSchedulingPolicy,
        )

        synced_uc_name = f"{CATALOG}.gold.market_share_snapshot"
        source_uc_name = f"{CATALOG}.gold.market_share"

        w.database.create_synced_database_table(
            synced_table=SyncedDatabaseTable(
                name=synced_uc_name,                       # UC name of the synced table
                database_instance_name=INSTANCE,
                logical_database_name=DB_NAME,             # lands in databricks_postgres
                spec=SyncedTableSpec(
                    source_table_full_name=source_uc_name,
                    primary_key_columns=["period", "region", "category", "brand"],
                    # SNAPSHOT for the demo (one-shot refresh); use TRIGGERED or
                    # CONTINUOUS for ongoing sync in production.
                    scheduling_policy=SyncedTableSchedulingPolicy.SNAPSHOT,
                    new_pipeline_spec=NewPipelineSpec(
                        storage_catalog=CATALOG,
                        storage_schema="gold",
                    ),
                ),
            )
        )
        print(f"Synced table requested: {synced_uc_name} <- {source_uc_name}")
    except Exception as e:
        print("Synced-table SDK call failed or already exists (verify API against "
              f"installed SDK): {str(e)[:300]}")
        print("If field names differ, create it in the UI (Catalog > Create > Synced "
              "table) or via resources/lakebase.yml, then re-run the validation below.")
else:
    print("enable_lakebase_serving=false — no synced table for this target.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate the synced table is readable from Postgres
# MAGIC `get_top_movers` reads `market_share_snapshot` UNQUALIFIED, so it must be on
# MAGIC the app SP's `search_path`. Synced tables land in a PG schema that mirrors
# MAGIC the UC schema; if the read below needs a schema prefix, either set the role's
# MAGIC `search_path` to include it, or update `state.top_movers` to qualify the name.

# COMMAND ----------

if ENABLE_SERVING and DB_HOST != "REPLACE_ME":
    try:
        with pg() as conn, conn.cursor() as cur:
            cur.execute("SELECT period, region, category, brand, share_pct "
                        "FROM market_share_snapshot ORDER BY share_pct DESC LIMIT 5")
            for row in cur.fetchall():
                print(row)
        print("Synced snapshot is readable — get_top_movers will work.")
    except Exception as e:
        print("Snapshot not yet readable (sync may still be running, or it landed in "
              f"a non-default schema): {str(e)[:300]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes
# MAGIC - **Fleet rollout:** parameterize `catalog` + `lakebase_host` and run this as a
# MAGIC   governed Job (run-as = app SP) across all customer instances — never a human
# MAGIC   in the console (managed Lakebase blocks `ALTER TABLE ... OWNER TO`).
# MAGIC - **Branch isolation:** to stage schema changes, run against a Lakebase branch
# MAGIC   first (see `lakebase_migration_notebook.py`) before replaying on production.
