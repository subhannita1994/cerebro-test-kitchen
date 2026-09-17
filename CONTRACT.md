# Cerebro Test Kitchen — Build Contract (data model & naming)

> Single source of truth for names, schemas, and variables. Both swimlanes
> (data + AI) and the DAB build against exactly these. Do not rename without
> updating this file.

## Catalogs (all in the one shared training workspace)

| DAB target | Catalog | Meaning |
|---|---|---|
| `dev` | `cerebro_dev` | The "everything" reference build |
| `customer_a` | `cerebro_a` | Full pipelines + full AI (3 tools) |
| `customer_b` | `cerebro_b` | Schema variation (omnichannel, no promo) + 2 tools |
| `customer_c` | `cerebro_c` | Code variation (same schema as A) + Lakebase feature |

**Every UC securable for a customer lives inside that customer's catalog.**

## Schemas (within each catalog)

`bronze`, `silver`, `gold`. UC functions live in `gold`. Genie reads `gold`.
A landing **volume** `gold.raw_landing` (or `bronze.landing`) holds source files for Auto Loader.

## Bundle variables (databricks.yml) — DATA side + app name

These drive the DATA resources (jobs/pipelines/functions), where `${var.*}` IS
substituted, plus the app's name. The APP's per-customer runtime config does NOT
come from bundle vars (app.yaml doesn't take `${var}`) — it lives in
`src/app/config/<customer>.yaml` and the app resolves it from `DATABRICKS_APP_NAME`
(see `## Additions` and `docs/deploy-notes.md`).

| Variable | Example | Drives |
|---|---|---|
| `catalog` | `cerebro_a` | Target catalog for all objects |
| `customer_slug` | `a` | Hyphen-safe app-name suffix (`cerebro-assistant-<slug>`); slug ∈ dev\|a\|b\|c |
| `warehouse_id` | `1230d4d180bc149c` | SQL warehouse for DDL + functions (+ app tools via `valueFrom`) |
| `enable_promo` | `true` / `false` | Promo pipeline + `f_promo_lift` function (data side) |
| `pipeline_variant` | `baseline` \| `omnichannel` \| `altlogic` | Which streaming code module runs |
| `enable_lakebase_serving` | `false` / `true` | Gates the `setup_lakebase` synced-table step (Customer C) |
| `secret_scope` | `cerebro_demo` | Persona SP creds |

Per-customer APP config (in `src/app/config/<customer>.yaml`, NOT bundle vars):
`catalog`, `claude_model` (UC AI Gateway model FQN), `genie_space_id`,
`enable_lakebase_serving`, `lakebase_*`, and the enabled `tools` list.

Target → variable matrix:

| Target | catalog | customer_slug | enable_promo | pipeline_variant | enable_lakebase_serving |
|---|---|---|---|---|---|
| dev | cerebro_dev | dev | true | baseline | true |
| customer_a | cerebro_a | a | true | baseline | false |
| customer_b | cerebro_b | b | false | omnichannel | false |
| customer_c | cerebro_c | c | true | altlogic | true |

## Source tables (landed as files → Auto Loader → bronze)

- **pos_sales**: `transaction_id STRING, store_id STRING, product_id STRING, qty INT, unit_price DECIMAL(10,2), txn_ts TIMESTAMP`
- **products** (dim): `product_id STRING, product_name STRING, category STRING, brand STRING`
- **stores** (dim): `store_id STRING, store_name STRING, region STRING, channel STRING`
- **promotions** (A/C only): `promo_id STRING, product_id STRING, discount_pct DECIMAL(5,2), start_date DATE, end_date DATE`
- **online_orders** (B only): `order_id STRING, product_id STRING, qty INT, unit_price DECIMAL(10,2), order_ts TIMESTAMP, fulfillment STRING`

## Bronze (streaming ingest, Auto Loader)

`bronze.pos_sales_raw`, `bronze.products_raw`, `bronze.stores_raw`,
`bronze.promotions_raw` (A/C), `bronze.online_orders_raw` (B) — raw + `_ingest_ts`, `_source_file`.

## Silver (cleaned/typed/dedup, streaming)

`silver.pos_sales`, `silver.products`, `silver.stores`, `silver.promotions` (A/C), `silver.online_orders` (B).

## Gold (market analytics)

- **gold.sales_daily**: `sales_date DATE, product_id STRING, store_id STRING, region STRING, category STRING, brand STRING, units BIGINT, revenue DECIMAL(18,2)`
- **gold.market_share**: `period STRING, region STRING, category STRING, brand STRING, brand_revenue DECIMAL(18,2), category_revenue DECIMAL(18,2), share_pct DECIMAL(6,3)`
- **gold.promo_performance** (A/C): `promo_id STRING, product_id STRING, baseline_units BIGINT, promo_units BIGINT, lift_pct DECIMAL(6,2)`
- **gold.omnichannel_sales** (B): `sales_date DATE, product_id STRING, channel STRING, units BIGINT, revenue DECIMAL(18,2)`

### Variant differences
- **omnichannel (B)**: NO `promotions`/`promo_performance`; ADD `online_orders` + `omnichannel_sales`; `sales_daily`/`market_share` include online channel.
- **altlogic (C)**: SAME schema as baseline; `market_share.share_pct` uses a **trailing-4-week revenue-weighted** methodology instead of point-in-period; different watermark/dedup window in silver. DDL identical to baseline.

## UC functions (in `${catalog}.gold`)

- `f_market_share(p_category STRING, p_region STRING, p_period STRING) RETURNS TABLE(...)` — reads `gold.market_share`.
- `f_promo_lift(p_product_id STRING, p_promo_id STRING) RETURNS TABLE(...)` — reads `gold.promo_performance`. **Only deployed when `enable_promo=true`.**

## Agent tools (config-driven, per `src/app/config/<customer>.yaml` → `tools:`)

1. `query_genie` — always on.
2. `get_market_share` → calls `${catalog}.gold.f_market_share`.
3. `get_promo_lift` → calls `${catalog}.gold.f_promo_lift`. **Absent for Customer B.**

Customer C additionally: `get_top_movers` (reads Lakebase synced snapshot, sub-second) and `save_to_watchlist` (writes Lakebase OLTP table).

## Lakebase (Postgres)

- Chat memory (all customers): `users`, `threads`, `messages`, `agent_state` (owned by app SP).
- Customer C serving: **synced table** `market_share_snapshot` (Delta `gold.market_share` → Lakebase); **OLTP** `watchlist(user_id, product_id, note, created_at)`.

## Additions (DATA swimlane — introduced by the data build, no renames)

These are conventions the data pipelines adopted that were implied but not
spelled out above. They add NO new columns to the contract's gold tables and no
new table names; they clarify housekeeping columns, sentinel values, and value
formats so the AI swimlane (Genie / tools) knows what it will see.

- **Bronze housekeeping columns**: in addition to `_ingest_ts` and `_source_file`,
  every `bronze.*_raw` table carries `_rescued_data STRING` (the Auto Loader
  rescued-data column for schema drift). Silver/gold are unaffected.
- **`market_share.period` / (implied) period key format** = ISO **year-week**
  string `"<yyyy>-W<ww>"`, e.g. `2026-W12` (`ww` zero-padded). Used by
  `f_market_share(p_period)`; pass `NULL` or `'latest'` for the most recent
  period. `f_market_share` / `f_promo_lift` also accept `NULL` or `'all'` on the
  category/region/product/promo filters to mean "no filter".
- **Omnichannel (Customer B) online rows in `gold.sales_daily` / `gold.market_share`**:
  because `sales_daily` has a `region`/`store_id` grain but no `channel` column,
  online e-commerce sales are folded in with sentinel values
  **`store_id = 'ONLINE'`** and **`region = 'Online'`**. So for Customer B,
  market share shows an extra `region = 'Online'` bucket. The per-channel detail
  lives in `gold.omnichannel_sales.channel`.
- **`channel` enumerated values**: `Grocery`, `Club`, `Foodservice` (in-store,
  from the store dim) and `Online` (Customer B e-commerce). Region enumerated
  values: `Northeast`, `Southeast`, `Midwest`, `Southwest`, `West` (+ `Online`
  sentinel for B). Category values: `Breads`, `Bagels`, `Pastries`, `Croissants`,
  `Muffins`.
- **Seed job parameterization**: `generate_market_data` derives
  `include_online_orders` from `pipeline_variant` (`omnichannel` ⇒ true) and
  `include_promotions` from `enable_promo`, so the DAB `seed_data` job passes
  only the contract variables. It supports `mode=files` (default; lands JSON into
  `${catalog}.gold.raw_landing/<source>/` for Auto Loader) and `mode=tables`
  (writes straight to bronze for a quick start).

## Additions

> Appended by the AI/APP swimlane build (see `src/app/`). New app-owned objects
> that weren't in the original contract; no existing name changed.

### App analytics table — `${catalog}.gold.agent_turn_log`
App-owned observability table (one row per user turn), written by `src/app/tracing.py`
as the **app SP** via the SQL Statement Execution API on `${var.warehouse_id}`. The
app creates it on first use (`CREATE TABLE IF NOT EXISTS`, idempotent), so it does
not depend on the data swimlane. Columns:

```
turn_id STRING, event_time TIMESTAMP, username STRING, persona STRING,
thread_id STRING, prompt STRING, routed_to_genie BOOLEAN, tools_used STRING,
claude_decision_s DOUBLE, genie_figure_out_s DOUBLE, genie_reply_s DOUBLE,
claude_final_s DOUBLE, total_s DOUBLE, genie_status STRING,
genie_error STRING, genie_sql STRING, answer STRING, claude_reasoning STRING
```
`tools_used` is a comma-separated, de-duplicated list of the tools invoked that
turn (new vs. the reference app, which only tracked Genie). Override the location
with env `CEREBRO_LOG_TABLE` if needed.

### Config injection — bundled yaml + runtime resolution (NOT `${var.*}` in app.yaml)
Databricks Apps read `app.yaml` env as STATIC values (the bundle does **not**
substitute `${var.*}` inside app.yaml), and only the `src/app/` folder is deployed
(repo-root files aren't reachable at runtime). So per-customer behavior is carried
by **bundled** config, resolved at runtime:

- **`src/app/config/{dev,customer_a,customer_b,customer_c}.yaml`** — the single home
  for each target's runtime config (`catalog`, `claude_model`, `genie_space_id`,
  `enable_lakebase_serving`, `tools`, `lakebase_instance|host|database`). Replaces
  the old repo-root `config/customer_*.yaml` (deleted).
- **`src/app/customer_config.py`** resolves the active customer:
  1. `CEREBRO_CUSTOMER` env (local-dev override) — slug in {dev, a, b, c}, else
  2. `DATABRICKS_APP_NAME` (auto-injected), format **`cerebro-assistant-<slug>`**,
     slug ∈ {dev, a, b, c} → `dev.yaml` / `customer_a.yaml` / `_b` / `_c`, else
  3. fall back to `dev.yaml` with a logged warning.
- **App name slug:** the DAB app resource is `name: cerebro-assistant-${var.customer_slug}`
  (hyphen-safe — app names reject `_`). `customer_slug` is set per target in
  databricks.yml (dev→`dev`, customer_a→`a`, customer_b→`b`, customer_c→`c`).

### App runtime env (static / injected only)
`src/app/app.yaml` sets just: `WAREHOUSE_ID` (via `valueFrom: sql-warehouse`, the
app resource) and `SECRET_SCOPE` (static `cerebro_demo`). `DATABRICKS_HOST`,
`DATABRICKS_CLIENT_ID`, and `DATABRICKS_APP_NAME` are auto-injected by the Apps
runtime. Lakebase connection details (instance/host/database) come from the bundled
customer yaml, not env.

### Synced-table PG reachability
`get_top_movers` reads `market_share_snapshot` **unqualified**, so the synced table
must be on the app SP's Postgres `search_path`. If it lands in a non-default schema,
either set the role's `search_path` or qualify the name in `src/app/state.top_movers`.
