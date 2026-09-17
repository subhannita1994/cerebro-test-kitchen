# Setup 01 — Provision catalogs, warehouse, secrets, personas

> One-time workspace prep by the organizer. ~20 min.

## 1. Create the four catalogs

All UC objects for a customer live inside that customer's catalog.

```sql
CREATE CATALOG IF NOT EXISTS cerebro_dev COMMENT 'Cerebro Test Kitchen — everything build';
CREATE CATALOG IF NOT EXISTS cerebro_a   COMMENT 'Customer A — full';
CREATE CATALOG IF NOT EXISTS cerebro_b   COMMENT 'Customer B — omnichannel, no promo';
CREATE CATALOG IF NOT EXISTS cerebro_c   COMMENT 'Customer C — alt logic + Lakebase';
```

Grant the workshop group on each (see setup/00 cheat-sheet). The `apply_ddl` job
creates the `bronze`/`silver`/`gold` schemas and the `gold.raw_landing` volume
inside each catalog — you don't create schemas by hand.

## 2. Serverless SQL warehouse

Create (or pick) a **serverless** SQL warehouse. Note its **ID** (from the
warehouse's Connection details / URL). You'll put it in `databricks.yml`
(`variables.warehouse_id`) or pass `--var warehouse_id=<id>` at deploy.

## 3. Secret scope + persona service principals

Cerebro runs data calls as a **persona service principal** so Unity Catalog
enforces per-persona access (mirrors production). Minimum: one persona; ideally two.

1. Create SPs: `cerebro-persona-analyst` (less privileged) and `cerebro-persona-manager` (full). Generate an OAuth secret (`client_id` + `client_secret`) for each.
2. Create a secret scope and store them:
   ```bash
   databricks secrets create-scope cerebro_demo
   databricks secrets put-secret cerebro_demo persona_a_client_id     --string-value <analyst_client_id>
   databricks secrets put-secret cerebro_demo persona_a_client_secret --string-value <analyst_secret>
   databricks secrets put-secret cerebro_demo persona_b_client_id     --string-value <manager_client_id>
   databricks secrets put-secret cerebro_demo persona_b_client_secret --string-value <manager_secret>
   ```
3. Grant the **app SP** `READ` on the scope: `databricks secrets put-acl cerebro_demo <app-sp> READ`.
4. Grant each persona SP the intended UC access on the customer catalogs (e.g. analyst = SELECT on a subset; manager = full) so the permission-difference demo works.

## 4. Fill in `databricks.yml` placeholders

Replace every `REPLACE_ME`:
- `workspace.host` on each target → your training workspace URL.
- `variables.warehouse_id.default` → the warehouse ID from step 2.
- `variables.genie_space_id.default` → set after you create the Genie space (setup/03 / Module 1). OK to leave until then.
- `variables.claude_model.default` → the Unity AI Gateway model-service FQN (setup/03 / Module 1).

> Tip: keep secrets/IDs out of Git — pass them with `--var` or a local
> `*.local.yml` target overlay if your policy requires.

## Done when
The four catalogs exist, the group has grants, the warehouse ID and host are in
`databricks.yml`, and the secret scope holds the persona creds.
Next: `setup/02_seed_data.md`.
