# Module 1 — Customer A: ship the full reference stack

**Goal:** deploy the complete Cerebro-lite solution into `cerebro_a` from this one
repo, with all three AI tools working. This is the reference every later module
varies from. **~90 min.**

**Teaches:** end-to-end DABs deploy to a catalog target, UC functions as agent
tools, Unity AI Gateway orchestration of a many-call agent.

> Two swimlanes work in parallel and integrate at the checkpoint. The **repo** is
> the source of truth; a single `bundle deploy -t customer_a` assembles both
> swimlanes' work into `cerebro_a`.
>
> **You build in-workspace with Genie Code.** Run notebooks/SQL/Genie in the
> browser; run the `databricks bundle …` commands below in the workspace **web
> terminal** (no laptop CLI, no auth step). This holds for every module.

---

## 🥖 Data swimlane (Tashay's team)

1. **Apply the schema** to `cerebro_a` from DDL artifacts:
   ```bash
   databricks bundle run apply_ddl -t customer_a
   ```
   Confirm `cerebro_a` now has `bronze` / `silver` / `gold` schemas + `gold.raw_landing`.
2. **Seed + run the pipeline** (baseline variant, includes promotions):
   ```bash
   databricks bundle run seed_data    -t customer_a
   databricks bundle run run_pipeline -t customer_a
   ```
3. **Register UC functions** (both, since `enable_promo=true`):
   ```bash
   databricks bundle run register_functions -t customer_a
   ```
4. **Verify gold + functions:**
   ```sql
   SELECT count(*) FROM cerebro_a.gold.sales_daily;
   SELECT * FROM cerebro_a.gold.f_market_share('Bagels','Northeast','2026-08');
   SELECT * FROM cerebro_a.gold.f_promo_lift('P0007','PR003');
   ```
   ✅ **Pass:** all three return rows.

## 🤖 AI swimlane (Hosein's team)

1. **Genie space** over `cerebro_a.gold` (import from `genie/genie_space.md`); put the space ID in the `customer_a` target (`--var genie_space_id=...` or `databricks.yml`).
2. **Unity AI Gateway model** — confirm the Claude model service FQN is set for `customer_a` (`claude_model`). (Reuse the one service across catalogs, or create per-catalog for isolated inference tables.)
3. **Deploy the app** with the Customer A tool list (`src/app/config/customer_a.yaml` → all 3 tools):
   ```bash
   databricks bundle deploy -t customer_a
   ```
4. **Test the app** (open its URL). Confirm each prompt routes correctly:
   - greeting → direct answer (no tool)
   - "market share for Bagels in the Northeast last month" → **get_market_share**
   - "did promo PR003 lift product P0007" → **get_promo_lift**
   - "which products are trending in Club stores" → **query_genie**

   ✅ **Pass:** all three tools fire; the app Logs tab shows persona + tool + timing.

---

## ✅ Module 1 done when
`cerebro_a` has populated gold tables, both UC functions return data, and the
Customer A app answers all four prompt types through the Gateway Claude model
using the correct tool each time.

## Talking point
One `bundle deploy -t customer_a` shipped pipelines + functions + app + Lakebase
together. To stand up the *next* customer, we change **variables**, not code.
