# Databricks notebook source
# MAGIC %md
# MAGIC # Grant the Cerebro app service principal its runtime access
# MAGIC
# MAGIC Backs the **`grant_app_sp`** DABs job. Grants the deployed app's service
# MAGIC principal exactly what it needs at runtime:
# MAGIC - **`READ`** on the persona secret scope (so it can mint persona SP tokens), and
# MAGIC - **`USE CATALOG`, `USE SCHEMA`, `SELECT`, `EXECUTE`, `MODIFY`, `CREATE TABLE`**
# MAGIC   on the customer catalog. SELECT/EXECUTE run the UC-function tools; MODIFY +
# MAGIC   CREATE TABLE let it write `gold.agent_turn_log` (tracing.py creates it on
# MAGIC   first write, then INSERTs). See the least-privilege note in the grant cell.
# MAGIC
# MAGIC ## Why a job (not a prerequisite)
# MAGIC A Databricks App's service principal is **created when the app is deployed**, so
# MAGIC these grants can only happen AFTER `bundle deploy`. Run this LAST:
# MAGIC `deploy → apply_ddl → seed_data → run_pipeline → register_functions →
# MAGIC setup_lakebase → create_genie_space → **grant_app_sp**`. It's idempotent, so
# MAGIC re-running is safe.
# MAGIC
# MAGIC ## Run-as
# MAGIC Run as a principal that can set the grants: **MANAGE on the secret scope** and
# MAGIC **owner / MANAGE GRANT on the catalog** (the workshop organizer/admin). The
# MAGIC warehouse + Lakebase grants are handled separately by the app *resources* in
# MAGIC `resources/app.yml`, so they are NOT repeated here.

# COMMAND ----------

dbutils.widgets.text("catalog", "cerebro_dev", "Target catalog")
dbutils.widgets.dropdown("customer_slug", "dev", ["dev", "a", "b", "c"], "Customer slug")
dbutils.widgets.text("secret_scope", "cerebro_demo", "Persona secret scope")
dbutils.widgets.text("app_name", "", "App name override (blank => cerebro-assistant-<slug>)")

CATALOG = dbutils.widgets.get("catalog").strip()
SLUG    = dbutils.widgets.get("customer_slug").strip().lower()
SCOPE   = dbutils.widgets.get("secret_scope").strip()
APP_NAME = dbutils.widgets.get("app_name").strip() or f"cerebro-assistant-{SLUG}"

print(f"catalog={CATALOG} app={APP_NAME} scope={SCOPE}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import AclPermission

w = WorkspaceClient()

_results = []
def record(step, status, detail=""):
    _results.append({"step": step, "status": status, "detail": str(detail)[:300]})
    print(f"[{status:6}] {step} — {detail}")

# COMMAND ----------

# --- Resolve the app's service principal (created at app-deploy time) ---------
# The App object exposes its SP: application (client) id, numeric id, and name.
app = w.apps.get(name=APP_NAME)
SP_CLIENT_ID = (getattr(app, "service_principal_client_id", None)
                or getattr(app, "service_principal_id", None))
SP_NAME = getattr(app, "service_principal_name", None)
assert SP_CLIENT_ID, (
    f"App '{APP_NAME}' has no service principal yet — deploy the app first "
    "(databricks bundle deploy) so its SP exists, then re-run this job.")
print(f"app SP: client_id={SP_CLIENT_ID} name={SP_NAME}")

# The UC grantee + secret-ACL principal for a service principal is its
# APPLICATION (client) id.
GRANTEE = str(SP_CLIENT_ID)

# COMMAND ----------

# --- 1. Secret scope READ (to mint persona SP tokens) -------------------------
try:
    w.secrets.put_acl(scope=SCOPE, principal=GRANTEE, permission=AclPermission.READ)
    record(f"secret scope '{SCOPE}' READ -> app SP", "OK", GRANTEE)
except Exception as e:
    record(f"secret scope '{SCOPE}' READ -> app SP", "ERROR",
           f"{e}. Ensure this job runs as someone with MANAGE on the scope, "
           f"or grant manually: databricks secrets put-acl {SCOPE} {GRANTEE} READ")

# COMMAND ----------

# --- 2. UC grants on the catalog (run UC-function tools + write turn log) ------
# What the app SP actually needs at runtime:
#   USE CATALOG + USE SCHEMA -> traverse the catalog/schemas
#   SELECT                   -> read gold (Genie/tool results)
#   EXECUTE                  -> call f_market_share / f_promo_lift
#   MODIFY                   -> INSERT into gold.agent_turn_log (write the turn log)
#   CREATE TABLE             -> tracing.py self-creates gold.agent_turn_log (CREATE
#                               TABLE IF NOT EXISTS) on first write
# (SELECT/EXECUTE alone — the four originally requested — run the tools but can't
#  write the log; MODIFY + CREATE TABLE are required for that.) GRANT is idempotent.
#
# LEAST-PRIVILEGE OPTION: pre-create gold.agent_turn_log in apply_ddl and drop the
# `CREATE TABLE` grant here — then the app SP needs only MODIFY on it. See the note
# in resources/governance.yml.
GRANTS = "USE CATALOG, USE SCHEMA, SELECT, EXECUTE, MODIFY, CREATE TABLE"
try:
    spark.sql(f"GRANT {GRANTS} ON CATALOG {CATALOG} TO `{GRANTEE}`")
    record(f"UC grants on {CATALOG} -> app SP", "OK", GRANTS)
except Exception as e:
    record(f"UC grants on {CATALOG} -> app SP", "ERROR",
           f"{e}. Run this job as the catalog owner / a principal with MANAGE GRANT.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(_results)[["step", "status", "detail"]]
n_err = (df["status"] == "ERROR").sum()
print(f"app SP: {GRANTEE}")
print("ALL GRANTS APPLIED" if n_err == 0 else f"{n_err} grant(s) FAILED — see rows below")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify (optional)
# MAGIC ```sql
# MAGIC SHOW GRANTS `<app-sp-client-id>` ON CATALOG <catalog>;
# MAGIC ```
# MAGIC ```bash
# MAGIC databricks secrets list-acls <scope>   # the app SP should show READ
# MAGIC ```
