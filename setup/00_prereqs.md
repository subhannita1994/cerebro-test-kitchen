# Setup 00 — Prerequisites & permissions

> Do this **before the workshop**. The organizer completes the "Organizer"
> items; every participant completes the "Participant" items. If you're the
> organizer testing this in your own FEVM workspace first, do both.

## How you'll build (read this first)

**You build entirely inside the training workspace — no local tools required.** You
author and run code with the in-workspace **Genie Code** agent (notebooks, SQL, UC,
Lakebase) against the repo checked out as a **Git folder**. The Genie **space** is
created from the `.md`-as-code files in `genie/`. The **only** command-line step is
the DABs deploy, which you run from the workspace **web terminal** (the `databricks`
CLI runs there under your workspace session — nothing to install on your laptop).

## Participant pre-work (10 min, before the day)

- [ ] Confirm you can open the **training workspace** in the browser and that **Genie Code** is available to you there.
- [ ] **Clone this repo as a Databricks Git folder** in your user workspace (Workspace → your folder → Create → Git folder → the repo URL). This — not a laptop clone — is where you work.
- [ ] Open a **web terminal** once (Compute → your cluster/serverless → Web terminal) and run `databricks bundle validate -t dev` from the repo folder to confirm it's clean. *(No auth step — the web terminal already runs as you.)*
- [ ] Read the 1-page architecture primer in `README.md` and skim `CONTRACT.md`.

> Local Databricks CLI on your laptop is **optional** — only needed if you'd rather
> run `databricks bundle` from your machine instead of the web terminal.

## Organizer / workspace prerequisites

You need **one shared training workspace** (the "mock Cerebro developer workspace").
Customers A/B/C are simulated as **separate catalogs in this one workspace** — we do
not deploy to separate workspaces.

### Workspace-level features enabled
- [ ] **Unity Catalog** metastore attached; ability to create catalogs (or pre-create the four catalogs — see setup/01).
- [ ] **Serverless SQL warehouse** available (used for DDL, UC functions, and the app).
- [ ] **Serverless compute for jobs/notebooks** (Structured Streaming runs serverless-triggered).
- [ ] **Unity AI Gateway** (GA) enabled, **and access to a Databricks-hosted Claude model** (Sonnet). This is the new UC-native AI Gateway — **not** the legacy per-endpoint serving tab.
- [ ] **Genie** enabled; participants can create/use Genie spaces.
- [ ] **Lakebase** enabled; permission to create a database instance.
- [ ] **Databricks Apps** enabled; permission to create an app.

### Identities & grants
- [ ] A **workshop group** (e.g. `cerebro-workshop`) containing all participants.
- [ ] On each of the four catalogs, grant the group: `USE CATALOG`, `USE SCHEMA`, `CREATE SCHEMA`, `CREATE TABLE`, `CREATE FUNCTION`, `CREATE VOLUME`, `SELECT`, `EXECUTE` (or `ALL PRIVILEGES` for the workshop). Details in setup/01.
- [ ] `CAN_USE` on the serverless SQL warehouse for the group.
- [ ] **Two persona service principals** (Analyst = less privileged, Manager = full) — mirrors real Cerebro's per-persona UC enforcement. Create the SPs and put their OAuth `client_id`/`client_secret` in a **secret scope** (default name `cerebro_demo`). (You can start single-persona and add the second later.)
- [ ] Grant each persona SP the UC access you want to **demonstrate** on the customer catalogs (analyst = a restricted subset; manager = full) so the per-persona difference is visible.

> **Grants that are NOT prerequisites (they happen after deploy).** The **app's own
> service principal** doesn't exist until the app is deployed, and the **Gateway
> model** doesn't exist until you create it — so grants that target them can't be
> done up front. These belong to app setup, not here: the app SP's secret-scope
> `READ` + catalog `USE`/`SELECT`/`EXECUTE` + Lakebase access, and the persona SPs'
> `CAN QUERY` on the model. See **setup/03 → "Grant the service principals (post-deploy)"**.

### Source-control
- [ ] A Git repo (GitHub or Azure DevOps) holding this starter repo, with the three customer variations tracked (branches or the `config/` + `ddl/variants/` + `pipeline_variant` mechanism in this repo). **No GitHub Actions are required** — DDL artifacts are applied per-catalog by the `apply_ddl` job.

## Permission cheat-sheet (SQL, run as metastore/catalog admin)

```sql
-- per catalog (repeat for cerebro_dev, cerebro_a, cerebro_b, cerebro_c)
GRANT USE CATALOG, USE SCHEMA, CREATE SCHEMA, CREATE TABLE, CREATE FUNCTION,
      CREATE VOLUME, SELECT, EXECUTE, MODIFY
  ON CATALOG <cat> TO `cerebro-workshop`;
-- warehouse: grant CAN_USE via the SQL Warehouses UI or permissions API.
-- secret scope: databricks secrets put-acl cerebro_demo <group-or-sp> READ
```

## Done when
`databricks bundle validate -t dev` succeeds and you can open the training
workspace, see the four catalogs, and run a query on the serverless warehouse.
Next: `setup/01_provision_catalogs.md`.
