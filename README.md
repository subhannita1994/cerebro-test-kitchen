# 🧠🥐 Cerebro Test Kitchen — One Recipe, Every Customer

A hands-on workshop starter repo. You'll build a lightweight but architecturally
faithful stand-in for **Cerebro** — FGF Brands' customer-facing market-analytics
AI agent — and deploy **three customer variations (A / B / C)** from this one
repo, learning the mechanics that make per-customer delivery scale to 1000+
customers: **DABs**, **DDL-artifact schema-as-code**, **Lakebase**, and the GA
**Unity AI Gateway**.

> **The big idea:** one repo (the recipe) → many customers (the bakes). The Git
> repo is the single source of truth; **DABs is the assembler** that packages the
> streaming pipelines + UC functions + app + Lakebase and ships them into each
> customer's catalog. `databricks bundle deploy -t customer_a` builds Customer A.

## What you're building (Cerebro-lite)

```
 POS / product / store / promo (+ online) source files
        │  (Auto Loader, Spark Structured Streaming)
        ▼
   bronze ──► silver ──► gold  (market analytics: sales_daily, market_share, promo_performance)
                               │            │
                       Genie space      UC functions (f_market_share, f_promo_lift)
                               │            │
                               └──► 3 tools ◄┘   +  Lakebase (chat memory; C: synced serving + watchlist)
                                        ▲
                    Claude (GA Unity AI Gateway model service)
                                        ▲
                          Databricks App (Streamlit chat)
```

## The three customers

| | Pipelines | Schema | AI tools | Special |
|---|---|---|---|---|
| **A** | full (incl. promotions) | baseline | Genie + market_share + promo_lift | reference build |
| **B** | omnichannel (adds online, no promo) | **different** (DDL overlay) | Genie + market_share (**no promo_lift**) | schema-as-code divergence |
| **C** | alt-logic (same schema as A) | same as A | all 3 **+ top_movers + watchlist** | **Lakebase** synced serving + OLTP |

## Repo layout

```
CONTRACT.md            ← data model + naming + variable matrix (source of truth)
databricks.yml         ← the DAB: 4 targets (dev/customer_a/b/c) → catalogs
ddl/                   ← versioned, parameterized DDL artifacts (schema-as-code)
  variants/customer_b/ ← Customer B's divergent schema overlay
src/
  data_gen/            ← synthetic bakery market data generator
  pipelines/           ← Spark Structured Streaming: baseline | omnichannel | altlogic
  functions/           ← UC SQL functions (f_market_share, f_promo_lift)
  app/                 ← the Streamlit chat app (Gateway Claude + config-driven tools)
    config/            ← per-customer runtime config (catalog/model/space/tools); app resolves by app name
  lakebase/            ← chat memory + Customer C synced snapshot & watchlist
genie/                 ← Genie-as-code: space definition, instructions, sample Qs
resources/             ← DAB resource files (pipelines/jobs/functions/app/lakebase)
setup/                 ← 00–03: prereqs → provision → seed → deploy baseline
modules/               ← the workshop's four module walkthroughs
docs/                  ← technical-plan.md + deploy-notes.md (verify-in-FEVM checklist)
docs/technical-plan.md ← how DABs/DDL/Lakebase/Gateway deploy each flavour
```

## How you build

**Everything is built inside the training workspace** — you author/run code with the
in-workspace **Genie Code** agent (notebooks, SQL, UC, Lakebase) against this repo
checked out as a **Git folder**, and the Genie **space** is seeded from the `.md`
files in `genie/`. The only command-line step is `databricks bundle deploy/run`,
which you run from the workspace **web terminal** (the CLI runs there as you — no
laptop install, no `auth login`).

## Quick start (see `setup/` for detail)

1. `setup/00_prereqs.md` — workspace access, Git folder, Genie Code, permissions.
2. `setup/01_provision_catalogs.md` — create `cerebro_dev` + `cerebro_a/b/c`, warehouse, secret scope, personas.
3. `setup/02_seed_data.md` — generate synthetic market data into `cerebro_dev`.
4. `setup/03_deploy_baseline.md` — `databricks bundle deploy -t dev` (in the web terminal) and smoke-test the app.
5. Then work the modules: `modules/module_1_customer_a.md` → `module_2` → `module_3` → (optional) `module_4`.

## Prerequisites at a glance

A training workspace with Unity Catalog, a serverless
SQL warehouse, **Unity AI Gateway** + access to Databricks-hosted Claude, **Genie**
(+ **Genie Code**), **Lakebase**, and **Databricks Apps** enabled; permission to
create catalogs (or four pre-created catalogs granted to your workshop group); a
**web terminal** for the DABs step (no local CLI required). Full checklist in
`setup/00_prereqs.md`.
