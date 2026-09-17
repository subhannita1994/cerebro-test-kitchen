# Deploy notes — verify in FEVM first (organizer)

This starter kit is meant to be **validated end-to-end in your own FEVM workspace
before sending to participants**. The items below are the known "fill these in /
confirm these against your workspace + CLI version" points. Work top to bottom;
each is quick once you know your workspace.

## 0. Web terminal runs the bundle (no laptop CLI)

Participants build in-workspace with **Genie Code** and run `databricks bundle
deploy/run` from the workspace **web terminal**. **Verify in FEVM:** open a web
terminal, `cd` into the repo's Git-folder path, and confirm `databricks bundle
validate -t dev` runs without a separate `auth login` (the terminal should already
act as you). If your workspace's web terminal needs the CLI installed or a token,
capture that one-time step for participants (or fall back to a local CLI for the
DABs step only). Everything else is pure browser + Genie Code.

## A. Fill-in placeholders (`REPLACE_ME`)

| Where | Value | When you know it |
|---|---|---|
| `databricks.yml` → each target `workspace.host` | training workspace URL | now |
| `databricks.yml` → `warehouse_id` | serverless SQL warehouse ID | setup/01 |
| `src/app/config/<customer>.yaml` → `claude_model` | UC AI Gateway model FQN | after you create the Gateway model (setup/03) |
| `src/app/config/<customer>.yaml` → `genie_space_id` | Genie space ID (per catalog) | after you create the space (setup/03) |
| `src/app/config/<customer>.yaml` → `lakebase_host` | Lakebase instance read/write host | after the Lakebase instance exists (Module 3) |

## B. App per-customer config injection (the important one)

**Verified contract:** Databricks Apps read env from `src/app/app.yaml` as *static*
values — the bundle does **not** substitute `${var.*}` inside app.yaml, and only
`src/app/` is deployed. So this kit does **not** push per-customer values through
bundle variables into the app. Instead:

- The app resolves its customer at runtime from the auto-injected
  **`DATABRICKS_APP_NAME`** (`cerebro-assistant-<slug>`, slug ∈ dev|a|b|c) and
  loads the bundled **`src/app/config/<customer>.yaml`** (catalog, claude_model,
  genie_space_id, tools, Lakebase flags).
- **Verify in FEVM:** deploy `-t customer_a` and `-t customer_b`, open each app's
  Logs, and confirm the startup line `tools enabled: …` differs (A has
  `get_promo_lift`; B does not). If `DATABRICKS_APP_NAME` isn't present in your
  runtime, set `CEREBRO_CUSTOMER` explicitly (the resolver honors it) or split
  into per-target source dirs. This is the #1 thing to confirm — it's what makes
  the config-driven-tools lesson real.
- App names are hyphen-only (no underscores) → the app is named with
  `${var.customer_slug}` (a/b/c/dev), not `${bundle.target}` (customer_a…).

## C. WAREHOUSE_ID via `valueFrom`

`src/app/app.yaml` gets the warehouse id from the app resource
(`resources/app.yml` → `sql-warehouse`) using `valueFrom`. Confirm the resource
name in app.yaml matches the one declared in `resources/app.yml`, and that the app
SP has `CAN_USE` on the warehouse.

## D. Lakebase (Customer C)

- The **synced-table** creation in `src/lakebase/setup_lakebase.py` and the
  `synced_database_tables` fields in `resources/lakebase.yml` are flagged to verify
  against your installed DAB/SDK version — the API surface for synced tables moves.
  The SQL-validation fallback in the notebook always works; use it if the resource
  API differs.
- DDL on Lakebase tables must run **as the owning app SP** (managed Lakebase blocks
  ownership reassignment) — see the pattern in the repo-root
  `../lakebase_migration_notebook.py`. Run `setup_lakebase` as the app SP.
- Set `lakebase_host` in the customer_c config once the instance is up.

## E. Unity AI Gateway model + Genie space

- Create the **Gateway model service** (GA UC-native AI Gateway, not legacy
  serving) fronting Databricks-hosted Claude Sonnet; put its FQN in the customer
  configs. Confirm the persona SPs have `CAN QUERY`.
- Create a **Genie space per catalog** over `<catalog>.gold` using
  `genie/genie_space.md`; put each space ID in that customer's config.

## F. Seeding / streaming caveats (from the data build)

- `generate_market_data` in `mode=files` rewrites part-files on re-run, so Auto
  Loader may re-ingest; silver dedup + gold `INSERT OVERWRITE` keep the end state
  correct. For a clean re-seed, clear the source folder + checkpoints, or use
  `mode=tables`.
- Pipelines run triggered (`availableNow`) so a workshop run terminates.

## G. Smoke test matrix (the pass/fail the modules check)

| Customer | Expect |
|---|---|
| A | gold populated; app fires all 3 tools; market-share AND promo-lift answered |
| B | `SHOW TABLES` shows omnichannel, no promo; `SHOW USER FUNCTIONS` has no `f_promo_lift`; app has 2 tools; promo question degrades gracefully |
| C | same schema as A, different `share_pct`; `get_top_movers` served from Lakebase; `save_to_watchlist` writes a row |

Once all three pass in FEVM, the kit is ready to send.
