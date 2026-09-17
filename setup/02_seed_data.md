# Setup 02 — Seed synthetic market data

> Generates deterministic bakery market-analytics data so Genie, the UC
> functions, and the app have something real to answer over. ~10 min.

The `seed_data` job runs `src/data_gen/generate_market_data.py`, which writes
source files into `${catalog}.gold.raw_landing/<source>/` (for Auto Loader) — or
straight to bronze in `tables` mode for a fast start.

> Run the `databricks bundle …` commands here in the workspace **web terminal**
> (no laptop CLI). You can also just run the `generate_market_data` notebook
> directly in-workspace with Genie Code instead of via the job.

## Baseline seed (dev / Customer A / Customer C)

Includes promotions. Run against `cerebro_dev` first to validate:

```bash
databricks bundle run seed_data -t dev
# or, ad hoc:
#   --var catalog=cerebro_dev --var enable_promo=true
```

Params (defaults in the script): `days=90`, `num_stores=40`, `num_products=60`,
`include_promotions` follows `enable_promo`, `include_online_orders` follows the
omnichannel variant, deterministic `seed`.

## Customer B seed (omnichannel, no promotions)

```bash
databricks bundle run seed_data -t customer_b   # enable_promo=false, adds online_orders
```

## What good looks like
- Files land under `<catalog>.gold.raw_landing/{pos_sales,products,stores,promotions|online_orders}/`.
- Sales show weekly seasonality and a couple of promo-driven spikes (so market-share and promo-lift are meaningful).
- Re-running is idempotent/deterministic (same seed → same data).

## Done when
`SELECT count(*)` on the landed files (or bronze tables) returns rows for each
source. Next: `setup/03_deploy_baseline.md`.
