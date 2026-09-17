# Cerebro Test Kitchen — Technical Deployment Plan

How **DABs**, **DDL-artifact schema-as-code**, **Lakebase**, and the GA **Unity AI
Gateway** combine to deploy the three customer flavours from one repo into one
shared training workspace. Companion to `CONTRACT.md` (data model) and the
`modules/` walkthroughs (hands-on steps).

## 1. Topology — one workspace, catalog-as-target

Everything runs in a single training workspace. Customers are simulated as
**separate catalogs**; DABs **targets map to catalogs**, not workspaces. All UC
securables for a customer live inside that customer's catalog.

| Target | Catalog | slug | enable_promo | pipeline_variant | lakebase_serving |
|---|---|---|---|---|---|
| dev | cerebro_dev | dev | true | baseline | true |
| customer_a | cerebro_a | a | true | baseline | false |
| customer_b | cerebro_b | b | false | omnichannel | false |
| customer_c | cerebro_c | c | true | altlogic | true |

> The app's per-customer config (catalog, `claude_model`, `genie_space_id`,
> enabled `tools`, Lakebase flags) is **not** a bundle variable — app.yaml doesn't
> take `${var}`. It lives in `src/app/config/<customer>.yaml`, and the app resolves
> which file to load from the auto-injected `DATABRICKS_APP_NAME`
> (`cerebro-assistant-<slug>`). See §6 and `deploy-notes.md`.

The **repo is the source of truth; DABs is the assembler.** `databricks bundle
deploy -t <target>` packages pipelines + UC functions + app + Lakebase and
deploys them together into that catalog. Two swimlanes (data / AI) edit different
folders and converge at deploy time.

## 2. DABs — one bundle, four targets

`databricks.yml` declares the variables and the four targets (see the matrix). A
variation is a **change of variables, never a code fork**:
- `catalog` → where objects land.
- `enable_promo` → deploy the promotions pipeline + `f_promo_lift` + gate the promo tool.
- `pipeline_variant` (`baseline|omnichannel|altlogic`) → which Structured Streaming module runs.
- `enable_lakebase_serving` → Customer C's synced snapshot + OLTP watchlist.
- `customer_slug` → the app's hyphen-safe name suffix, which the app reads back to load its config.

Resources (in `resources/*.yml`): `seed_data`, `apply_ddl`, `run_pipeline`,
`register_functions` (jobs); the UC functions; the Databricks App; the Lakebase
instance (+ synced table for C). Deploy assembles them; `bundle run <job>`
executes each step.

## 3. Schema-as-code via DDL artifacts (no GitHub Actions)

`ddl/` holds numbered, idempotent, catalog-parameterized `.sql` artifacts. The
`apply_ddl` job substitutes `${catalog}` and executes each against the target
catalog — the schema is code, versioned in Git, applied per catalog.

- **Propagation:** the same base artifacts applied to every catalog → consistent
  schema across the fleet.
- **Divergence (Customer B):** `apply_ddl` applies the base then the overlay
  `ddl/variants/customer_b/` (omit promotions/promo_performance, add
  online_orders/omnichannel_sales) when `pipeline_variant=omnichannel`. This is
  the teachable "this customer's schema is different" moment — auditable and
  repeatable, and CI-ready if they later wire GitHub Actions/Azure DevOps.

## 4. The three flavours

### Customer A — full
Baseline DDL + baseline pipeline + both UC functions + all 3 tools. The reference.

### Customer B — schema variation + one fewer tool
Divergent DDL overlay + omnichannel pipeline; `enable_promo=false` means
`f_promo_lift` is never created and `get_promo_lift` is absent from
`customer_b.yaml`. Genie sees the online channel. App degrades gracefully on
promo questions.

### Customer C — code variation + Lakebase
DDL **identical to A**; `pipeline_variant=altlogic` swaps the market-share
methodology (trailing-4-week revenue-weighted) and silver watermark — schema
unchanged, numbers change. `enable_lakebase_serving=true` adds:
- **Synced table** `market_share_snapshot`: Delta `gold.market_share` → Lakebase
  Postgres (reverse-ETL) for sub-second in-chat reads (`get_top_movers`).
- **OLTP** `watchlist`: the chat writes rows (`save_to_watchlist`), owned by the app SP.

## 5. Unity AI Gateway (GA)

The app calls Claude through the **GA Unity AI Gateway** —
`POST {host}/ai-gateway/mlflow/v1/chat/completions` with a **UC model-service
FQN** (`claude_model`), not the legacy per-endpoint serving tab. The gateway is
the single control plane over the orchestrator's many calls per turn: usage/cost,
payload logging (inference table), optional guardrail policies, rate limits, and
routing/fallback. Per-customer cost attribution comes from the persona SP as
`requester`. Deep-dive in `modules/module_4`.

## 6. Agent tool orchestration

The app sends Claude an OpenAI-style tool schema built from the resolved customer
config's `tools:` list (`src/app/config/<customer>.yaml`). Claude
decides per prompt which tool to call; the app executes:
- `query_genie` → Genie Conversation API as the persona SP.
- `get_market_share` / `get_promo_lift` → `${catalog}.gold.f_*` via the SQL
  Statement Execution API on the serverless warehouse, as the persona SP (UC
  enforces access).
- (C) `get_top_movers` → Lakebase synced snapshot; `save_to_watchlist` → Lakebase OLTP.
Tools follow config, so the **same app image** behaves differently per customer.

## 7. Verification (organizer runs in FEVM before sending)

End-to-end in your own FEVM workspace:
1. `bundle validate`; create 4 catalogs; seed `cerebro_dev`; deploy `dev`; smoke-test app (all 4 prompt types).
2. **A:** `bundle deploy -t customer_a` + run jobs → gold populated, all 3 tools fire, chat answers market-share **and** promo-lift.
3. **B:** `apply_ddl -t customer_b` (overlay) + `deploy -t customer_b` → `SHOW TABLES` proves schema differs (omnichannel present, promo absent), `SHOW USER FUNCTIONS` shows no `f_promo_lift`, app has 2 tools and handles promo questions gracefully, Genie sees online sales.
4. **C:** `deploy -t customer_c` (altlogic) → `DESCRIBE` shows A's schema, `share_pct` differs; `setup_lakebase` creates the synced snapshot + watchlist; app serves top-movers sub-second and writes a watchlist row.
5. **Gateway:** inference table logs per-persona calls; `observability_queries.sql` shows cost/latency per customer.
6. (Optional) branch Customer C's Lakebase, test a migration, confirm prod untouched.

Each module `.md` carries these as ✅ pass/fail lines so participants self-verify
in the room.

## 8. Learning-objective → mechanism map

| Objective | Where it's exercised |
|---|---|
| DABs multi-target deploy | every module; `databricks.yml` + `resources/*.yml` |
| DDL schema-as-code + divergence | `ddl/` + `apply_ddl`; Module 2 overlay |
| Config-driven tool gating | `config/*.yaml` + `src/app/tools.py`; Module 2 |
| Code variation w/o schema change | `pipeline_variant=altlogic`; Module 3 |
| Lakebase synced tables + OLTP | `src/lakebase/`; Module 3; Module 4-B branching |
| Unity AI Gateway governance | `src/app/agent.py` Gateway path; Module 4-A |
