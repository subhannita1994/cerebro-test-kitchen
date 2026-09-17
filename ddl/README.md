# DDL artifacts — schema-as-code for the Cerebro Test Kitchen

This folder is the **schema-as-code** half of the workshop. Every Unity Catalog
object (schemas, volume, tables) is defined as a **numbered, idempotent,
catalog-parameterized `.sql` artifact**. There is **no GitHub Actions** in this
starter — the `apply_ddl` DAB job runs a tiny runner notebook
(`src/pipelines/apply_ddl.py`) that:

1. reads the target catalog from `${var.catalog}` (a widget/arg),
2. substitutes the literal `${catalog}` token in each `.sql` file, and
3. executes each statement with `spark.sql(...)`, in filename order.

Because catalogs map 1:1 to customers (`cerebro_a`, `cerebro_b`, `cerebro_c`,
`cerebro_dev`), the SAME artifacts are applied **per catalog** — that is how one
codebase stamps out four customer schemas.

## Files (applied in order)

| File | Applies to | Creates |
|---|---|---|
| `000_catalog_schemas.sql` | all | `bronze`, `silver`, `gold` schemas + volume `gold.raw_landing` |
| `001_bronze.sql` | all (promo table used by A/C) | `bronze.*_raw` tables |
| `002_silver.sql` | all (promo table used by A/C) | `silver.*` tables |
| `003_gold.sql` | all (promo table used by A/C) | `gold.sales_daily`, `gold.market_share`, `gold.promo_performance` |
| `variants/customer_b/010_omnichannel.sql` | **Customer B only** | overlay — see below |

## Idempotent + re-runnable

Everything uses `CREATE ... IF NOT EXISTS`. Re-applying is a no-op, so the job
is safe to re-run at the start of every workshop. The Customer B overlay uses
`DROP TABLE IF EXISTS` for the promo objects it removes, so it too is
re-runnable.

## Parameterization

The `.sql` files contain the literal token `${catalog}` (NOT a Spark/SQL
variable). The runner replaces it with the deploy-time catalog name before
execution. The files also open with `USE CATALOG ${catalog};` so unqualified
names resolve correctly and the pattern is demonstrated both ways.

## The Customer B divergence (schema-as-code exercise)

Customers **A** and **C** share the baseline schema (`000`–`003`). Customer
**C** even keeps the DDL *identical* to A — only its pipeline *code* differs
(trailing-4-week market share). That is the **code-variation, not
schema-variation** teaching point.

Customer **B** genuinely needs a **different schema** (omnichannel, no promo),
so it gets a single overlay applied *after* the base files:
`variants/customer_b/010_omnichannel.sql`. The overlay:

- **drops** the promo objects the base files created
  (`bronze.promotions_raw`, `silver.promotions`, `gold.promo_performance`),
- **adds** `bronze.online_orders_raw`, `silver.online_orders`, and
  `gold.omnichannel_sales`, and
- leaves `gold.sales_daily` / `gold.market_share` shapes unchanged — but their
  *rows* now include the **Online** channel (a transform difference, handled in
  `src/pipelines/omnichannel/pipeline.py`, not a DDL difference).

The `apply_ddl` runner applies the overlay only when
`${var.pipeline_variant} == omnichannel`. This is the crisp illustration of
**divergent schema managed as code**: base artifacts + a small, reviewable,
idempotent overlay — no fork, no drift.
