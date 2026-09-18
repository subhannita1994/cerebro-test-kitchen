# Setup 03 — Deploy the baseline `dev` build & smoke-test

> Stand up the full "everything" reference solution in `cerebro_dev` so everyone
> has a known-good target to compare against. ~25 min. Organizer does this before
> the day; participants repeat the pattern per customer in the modules.

> **Where you run `databricks bundle …`:** in the workspace **web terminal**
> (Compute → Web terminal), from the repo's Git-folder directory. The CLI runs
> there under your workspace session — no laptop install, no `auth login`. (A
> local CLI works too if you prefer.) Everything else — notebooks, SQL, Genie,
> the app — is built in-workspace with **Genie Code**.

## 1. Deploy the bundle to `dev`

From the repo folder in the web terminal:

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
```

This deploys, into `cerebro_dev`, the resources wired in `resources/*.yml`:
the jobs (apply_ddl, seed_data, run_pipeline, register_functions, setup_lakebase),
the Lakebase **instance**, and the app.

> **Deploy must come first, and it must NOT depend on data-plane objects.** The
> jobs are *created* by `deploy`, so nothing can `bundle run` before deploy. And a
> resource that references `gold` (e.g. a synced table) can't be a deploy-time
> resource — `gold` doesn't exist until `apply_ddl` runs. That's why the synced
> table is created by the `setup_lakebase` **job** (step 2, last), not declared in
> the bundle. If `deploy` ever errors with *"schema gold does not exist"*, a
> resource is referencing `gold` too early.

## 2. Run the jobs — in this exact order

```bash
databricks bundle run apply_ddl          -t dev  # 1. creates bronze/silver/GOLD schemas + volume + empty tables
databricks bundle run seed_data          -t dev  # 2. generate synthetic source data
databricks bundle run run_pipeline       -t dev  # 3. Structured Streaming bronze→silver→gold (populates gold)
databricks bundle run register_functions -t dev  # 4. f_market_share (+ f_promo_lift when enable_promo)
databricks bundle run setup_lakebase     -t dev  # 5. chat tables (+ synced table now that gold is populated)
```

**Order matters:** `apply_ddl` is what makes the `gold` schema exist — every later
step reads/writes `gold`, so it must be first. `setup_lakebase` must be last (its
synced table's source is the now-populated `gold.market_share`) and must **run as
the app service principal** (set the job's *Run as* to the app SP — see setup/03 §4).

Verify gold populated:
```sql
SELECT * FROM cerebro_dev.gold.market_share  LIMIT 20;
SELECT * FROM cerebro_dev.gold.promo_performance LIMIT 20;
SELECT * FROM cerebro_dev.gold.f_market_share('Breads','Midwest','2026-08');
```

## 3. Create the Genie space, then wire the model + Genie into config

**Create the Genie space FIRST** — you can't put a `genie_space_id` in the config
until the space exists. Run the genie-as-code job (it builds the space over
`cerebro_dev.gold` from `genie/genie_space.md` and **prints the `space_id`**):

```bash
databricks bundle run create_genie_space -t dev
```

Then wire `src/app/config/dev.yaml`:
- `genie_space_id:` → the id the job printed.
- `claude_model: system.ai.claude-sonnet-4-5` → the ready-to-use **Unity AI
  Gateway** FQN. Nothing to create; the app calls
  `POST {host}/ai-gateway/mlflow/v1/chat/completions` with this model. (Only build
  your *own* model service if you want a dedicated inference table / guardrails —
  that's the Module 4 governance exercise.)
- Grant the **persona SPs `CAN QUERY`** on the model (Catalog Explorer → `system` →
  `ai` → the model → Permissions), and enrich the Genie space in the UI per
  `genie/genie_space.md` (trusted functions, synonyms, benchmarks).

## 4. Grant the service principals (post-deploy, via DABs)

The app SP is **created when the app is deployed** (step 1), so all SP grants
happen here — coded as the **`grant_service_principals` job**, not manual clicks:

```bash
databricks bundle run grant_service_principals -t dev
```

It grants both the **app SP** and the two **persona SPs**:
- **App SP:** `READ` on the secret scope + `USE CATALOG, USE SCHEMA, SELECT, EXECUTE, MODIFY, CREATE TABLE` on the catalog (tools + write `gold.agent_turn_log`).
- **Persona SPs** (the model / Genie / UC-function calls run *as* these): `EXECUTE` on the `system.ai` Gateway model, warehouse `CAN_USE`, Genie-space `CAN_RUN` (best-effort via API; manual fallback logged), and catalog data — **manager = full, analyst = restricted** (analyst gets `sales_daily`/`market_share` + `f_market_share` but NOT the promo table/function, so a promo question to the analyst is denied by UC — the per-persona enforcement demo).

Run it **as a principal with MANAGE on the scope, owner/MANAGE GRANT on the
catalog, and grant rights on `system.ai`** (the organizer/admin). Idempotent;
**repeats per app** (`dev`, `customer_a/b/c`). The app's **warehouse + Lakebase**
access is NOT here — it's granted declaratively by the app `resources` block in
`resources/app.yml` at deploy.

> **Lakebase chat tables:** you do **not** create these here. The app creates its
> own `users`/`threads`/`messages`/`agent_state` (and Customer-C `watchlist`) at
> startup via `state.init_schema()` — and the app runs **as the app SP**, so they're
> owned correctly. `setup_lakebase` (step earlier) runs as *you*, so use it only for
> the Customer-C **synced table**; if you let it create the chat tables while running
> as yourself, they'd be owned by your role, not the app SP.

## 5. Start the app, then smoke-test it

`bundle deploy` only **creates** the app and syncs its source — it does not start
it. Deploy (start) the app's code from the bundle with:

```bash
databricks bundle run cerebro_assistant -t dev
```

`cerebro_assistant` is the app resource key (`resources/app.yml`); this deploys
from `source_code_path: src/app` and starts the app — **do not** use the Apps-UI
"Deploy" button (it prompts you to attach source because it doesn't know the
bundle's path). Re-run this command whenever you change app code or `dev.yaml`.

Then open the app URL (from the Apps UI or `databricks apps list`). Try:
- "hello" → answers directly (no tool).
- "what's our market share for Breads in the Midwest last month?" → calls **get_market_share** (UC function).
- "how did promo PROMO123 lift sales for product X?" → calls **get_promo_lift**.
- "what are customers buying most in Club stores?" → calls **query_genie**.
Check the app **Logs** tab — you'll see the persona, the tool chosen, and timings.

## Done when
The dev app answers all four prompt types, using the Gateway Claude model and the
right tool each time. You now have a golden reference. Next: run the modules.

## Organizer FEVM test-first checklist
Before sending to participants, in your **own FEVM workspace**, run this whole
setup **and all three module deploys** end to end — see `docs/technical-plan.md`
§Verification. Confirm: A has all 3 tools; B's schema differs and lacks
`get_promo_lift`; C serves from Lakebase and writes a watchlist row.
