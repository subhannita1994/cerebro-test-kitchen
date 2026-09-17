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
pipelines/jobs (seed, apply_ddl, run_pipeline, register functions), the UC
functions, the Lakebase instance, and the app.

## 2. Apply DDL, run the pipeline, register functions

```bash
databricks bundle run apply_ddl        -t dev   # creates bronze/silver/gold + volume
databricks bundle run seed_data        -t dev   # (if not already seeded)
databricks bundle run run_pipeline     -t dev   # Structured Streaming bronze→silver→gold (triggered)
databricks bundle run register_functions -t dev # f_market_share (+ f_promo_lift since enable_promo=true)
```

Verify gold populated:
```sql
SELECT * FROM cerebro_dev.gold.market_share  LIMIT 20;
SELECT * FROM cerebro_dev.gold.promo_performance LIMIT 20;
SELECT * FROM cerebro_dev.gold.f_market_share('Breads','Midwest','2026-08');
```

## 3. Create the Genie space + Unity AI Gateway model (one-time)

- **Genie space** over `cerebro_dev.gold` — use `genie/genie_space.md` for the
  instructions, sample questions, and certified answers. Copy the space ID into
  `databricks.yml` (`genie_space_id`).
- **Unity AI Gateway model service** fronting Databricks-hosted Claude Sonnet —
  create it as a UC model service (the GA AI Gateway, not legacy serving). Copy
  its FQN into `databricks.yml` (`claude_model`). The app calls
  `POST {host}/ai-gateway/mlflow/v1/chat/completions` with this model.

## 4. Smoke-test the app

```bash
databricks bundle run --help    # (apps deploy as part of `bundle deploy`)
```
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
