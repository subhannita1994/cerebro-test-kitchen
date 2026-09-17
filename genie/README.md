# Genie-as-code — Cerebro Market Analytics

This folder is the **source of truth for the Genie space(s)** the app calls via
the `query_genie` tool. One space per customer catalog, all built from the single
definition in [`genie_space.md`](./genie_space.md).

## Why "as code"?

The workshop lesson is the same one the app teaches: **behavior is config, not a
code fork.** `genie_space.md` is the one buildable contract; you stamp it into
`cerebro_a` / `cerebro_b` / `cerebro_c` with the per-customer differences noted
inline (Customer B: omnichannel, no promo; Customer C: trailing-4-week share).

## Files

| File | What it is |
|---|---|
| `genie_space.md` | Buildable space contract: data sources, curated instructions, trusted assets, joins, ~10 sample questions, and certified benchmark Q&A. Follows `cerebro-genie-best-practices.md` §1b. |
| `README.md` | This file — how to create/import a space per catalog. |

## How to create a space per catalog

You need the target catalog's `gold` tables + UC functions to exist first (data
swimlane). Then, per customer:

### Option A — UI (fastest for the workshop)
1. **Genie → New space**, pick the **serverless** warehouse (`${var.warehouse_id}`).
2. Add the tables listed in `genie_space.md` **for that customer** (B swaps
   `promo_performance` → `omnichannel_sales`).
3. Paste the **general-instruction block**, add the **synonyms / entity matching**,
   declare the **joins**, and register the **trusted-asset functions**
   (`f_market_share` for all; `f_promo_lift` for A/C).
4. Load the **benchmark Q&A** into the Benchmarks tab and run a baseline score.
5. Copy the space id into the bundle as `${var.genie_space_id}` for that target.

### Option B — API / SDK (repeatable across catalogs)
Use the Genie management API / `databricks-genie` skill to create the space, add
data sources + instructions, and register trusted assets programmatically, reading
the fields from `genie_space.md`. Parameterize `${catalog}` so the same script
produces the A/B/C spaces. Export a tuned space and re-import it into another
catalog to clone curation. Store each resulting space id against its target.

> **Import/migrate between workspaces or catalogs:** export the space definition,
> find-and-replace the catalog name, and import into the target. The
> `databricks-genie` skill covers export/import + migration.

## Tuning loop (do this before demo day)

Per `cerebro-genie-best-practices.md`:
1. Confirm the warehouse is **serverless** (biggest latency lever).
2. Load **≥5 benchmark questions with expected answers** and score the space.
3. Apply accuracy levers in order: data prep/metric views → knowledge store
   (descriptions/synonyms/entity matching) → joins → SQL expressions + trusted
   functions → example SQL → the single general-instruction block.
4. Re-score after any material change (new columns, data refresh, new model
   version) to catch regressions. Never paste benchmark questions as example SQL.

## Contract linkage

- Space id → `${var.genie_space_id}` (databricks.yml) → app env `GENIE_SPACE_ID`.
- The app runs Genie **as the persona service principal** (auth.py), so Unity
  Catalog enforces per-persona access — hiding columns is not a security boundary
  (UC grants are). See `cerebro-genie-best-practices.md` §3b security note.
