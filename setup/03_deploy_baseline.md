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

## 3. Create the Genie space + Unity AI Gateway model (one-time)

- **Genie space** over `cerebro_dev.gold` — use `genie/genie_space.md` for the
  instructions, sample questions, and certified answers. Copy the space ID into
  the customer's `src/app/config/<customer>.yaml` (`genie_space_id`).
- **Unity AI Gateway model service** fronting Databricks-hosted Claude Sonnet —
  create it as a UC model service (the GA AI Gateway, not legacy serving). Copy
  its FQN into `src/app/config/<customer>.yaml` (`claude_model`). The app calls
  `POST {host}/ai-gateway/mlflow/v1/chat/completions` with this model.
- Grant the **persona SPs `CAN QUERY`** on this model service (they couldn't be
  granted earlier — the model didn't exist until now).

## 4. Grant the service principals (post-deploy)

The app's service principal is **created when the app is deployed** (step 1), so
its grants happen here, not in prerequisites. **This repeats per app** — each
target (`dev`, `customer_a/b/c`) is its own app `cerebro-assistant-<slug>` with its
own SP. For the app you just deployed:

- **Secret scope** — the app SP mints persona tokens, so it needs to read the scope:
  ```bash
  databricks secrets put-acl cerebro_demo <app-sp-id> READ
  ```
  (Find the app SP via the Apps UI → the app → "App resources"/permissions, or `databricks apps get cerebro-assistant-dev`.)
- **Unity Catalog** — grant the app SP `USE CATALOG`, `USE SCHEMA`, `SELECT`, `EXECUTE` on the customer catalog (it runs the UC-function tools + writes the turn log).
- **Warehouse & Lakebase** are already handled: they're declared as **app resources** in `resources/app.yml`, so the bundle grants the app SP access to them on deploy. (You could automate the secret grant the same way by adding a `secret` app resource — see deploy-notes.)

## 5. Smoke-test the app

Open the app URL (from the Apps UI or `databricks apps list`). Try:
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
