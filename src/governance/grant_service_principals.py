# Databricks notebook source
# MAGIC %md
# MAGIC # Grant the Cerebro service principals their runtime access
# MAGIC
# MAGIC Backs the **`grant_service_principals`** DABs job. Grants, all in code, the
# MAGIC principals the deployed solution runs as:
# MAGIC
# MAGIC **App SP** (`cerebro-assistant-<slug>`'s SP — reads secrets, writes the turn log):
# MAGIC - `READ` on the persona secret scope
# MAGIC - `USE CATALOG, USE SCHEMA, SELECT, EXECUTE, MODIFY, CREATE TABLE` on the catalog
# MAGIC   (SELECT/EXECUTE for tools; MODIFY + CREATE TABLE write `gold.agent_turn_log`)
# MAGIC
# MAGIC **Persona SPs** (the Genie / UC-function calls actually run AS these):
# MAGIC - SQL warehouse `CAN_USE` (functions + Genie execute here as the persona)
# MAGIC - Genie space `CAN_RUN` (best-effort via API; manual fallback printed)
# MAGIC - `cerebro_*` catalog data — **manager = full**, **analyst = restricted**
# MAGIC   (analyst gets `sales_daily`/`market_share` + `f_market_share` but NOT the
# MAGIC   promo table/function → asking analyst a promo question is denied by UC, which
# MAGIC   is the per-persona enforcement demo).
# MAGIC
# MAGIC ## Run AFTER `bundle deploy` (app SP exists then), as a principal with MANAGE on
# MAGIC the scope + owner/MANAGE GRANT on the catalog + grant rights on `system.ai`.
# MAGIC Idempotent; repeats per app/customer.

# COMMAND ----------

dbutils.widgets.text("catalog", "cerebro_dev", "Customer catalog")
dbutils.widgets.dropdown("customer_slug", "dev", ["dev", "a", "b", "c"], "Customer slug")
dbutils.widgets.text("secret_scope", "cerebro_demo", "Persona secret scope")
dbutils.widgets.text("warehouse_id", "", "Serverless SQL warehouse ID")
dbutils.widgets.text("genie_space_id", "", "Genie space id (blank => read from config)")
dbutils.widgets.text("app_name", "", "App name override (blank => cerebro-assistant-<slug>)")
dbutils.widgets.text("analyst_sp_name", "cerebro-persona-analyst", "Analyst persona SP display name")
dbutils.widgets.text("manager_sp_name", "cerebro-persona-manager", "Manager persona SP display name")

CATALOG   = dbutils.widgets.get("catalog").strip()
SLUG      = dbutils.widgets.get("customer_slug").strip().lower()
SCOPE     = dbutils.widgets.get("secret_scope").strip()
WAREHOUSE = dbutils.widgets.get("warehouse_id").strip()
GENIE_SPACE_ID = dbutils.widgets.get("genie_space_id").strip()
APP_NAME  = dbutils.widgets.get("app_name").strip() or f"cerebro-assistant-{SLUG}"
ANALYST_SP_NAME = dbutils.widgets.get("analyst_sp_name").strip()
MANAGER_SP_NAME = dbutils.widgets.get("manager_sp_name").strip()

# COMMAND ----------

import json
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import AclPermission

w = WorkspaceClient()

_results = []
def record(step, status, detail=""):
    _results.append({"step": step, "status": status, "detail": str(detail)[:300]})
    print(f"[{status:6}] {step} — {detail}")

def grant_sql(sql: str, label: str):
    try:
        spark.sql(sql)
        record(label, "OK", sql)
    except Exception as e:
        record(label, "ERROR", f"{e} | {sql}")

# COMMAND ----------

# --- resolve principals -------------------------------------------------------
# App SP (created at app-deploy time).
app = w.apps.get(name=APP_NAME)
APP_SP = str(getattr(app, "service_principal_client_id", None)
             or getattr(app, "service_principal_id", None))
assert APP_SP and APP_SP != "None", (
    f"App '{APP_NAME}' has no service principal yet — deploy the app first.")

def resolve_sp(display_name: str):
    found = list(w.service_principals.list(filter=f'displayName eq "{display_name}"'))
    if not found:
        record(f"resolve persona SP '{display_name}'", "ERROR", "not found — create it (setup/01)")
        return None
    return str(found[0].application_id)

ANALYST_SP = resolve_sp(ANALYST_SP_NAME)
MANAGER_SP = resolve_sp(MANAGER_SP_NAME)
print(f"app SP={APP_SP} analyst={ANALYST_SP} manager={MANAGER_SP}")

# Genie space id: fall back to the bundled per-customer config.
if not GENIE_SPACE_ID:
    _cfg_file = "dev.yaml" if SLUG == "dev" else f"customer_{SLUG}.yaml"
    for base in (os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd(), os.getcwd()):
        d = base
        for _ in range(8):
            p = os.path.join(d, "src", "app", "config", _cfg_file)
            if os.path.exists(p):
                import yaml
                GENIE_SPACE_ID = (yaml.safe_load(open(p)) or {}).get("genie_space_id", "")
                break
            d = os.path.dirname(d)
        if GENIE_SPACE_ID:
            break
print(f"genie_space_id={GENIE_SPACE_ID or '(unknown)'}")

# COMMAND ----------

# --- 1. APP SP ----------------------------------------------------------------
try:
    w.secrets.put_acl(scope=SCOPE, principal=APP_SP, permission=AclPermission.READ)
    record(f"secret scope '{SCOPE}' READ -> app SP", "OK", APP_SP)
except Exception as e:
    record(f"secret scope '{SCOPE}' READ -> app SP", "ERROR",
           f"{e}. Need MANAGE on the scope (or: databricks secrets put-acl {SCOPE} {APP_SP} READ)")

grant_sql(
    f"GRANT USE CATALOG, USE SCHEMA, SELECT, EXECUTE, MODIFY, CREATE TABLE "
    f"ON CATALOG {CATALOG} TO `{APP_SP}`",
    "app SP -> catalog (tools + write turn log)",
)

# COMMAND ----------

# --- 2. PERSONA SPs -----------------------------------------------------------
# Shared: warehouse CAN_USE + Genie CAN_RUN. Data grants differ per persona.
# NOTE: we do NOT grant model access here. The Gateway model
# system.ai.databricks-claude-sonnet-4-5 lives in the Databricks-managed `system`
# catalog — you can't GRANT on it, and it already grants EXECUTE to "All account
# users". If your SPs still can't query it, front it with your OWN model service
# (a securable you own) and grant EXECUTE on that instead.
def grant_warehouse(sp: str, who: str):
    if not WAREHOUSE:
        record(f"{who} -> warehouse CAN_USE", "WARN", "no warehouse_id provided")
        return
    try:
        w.api_client.do("PATCH", f"/api/2.0/permissions/warehouses/{WAREHOUSE}",
                        body={"access_control_list": [
                            {"service_principal_name": sp, "permission_level": "CAN_USE"}]})
        record(f"{who} -> warehouse CAN_USE", "OK", WAREHOUSE)
    except Exception as e:
        record(f"{who} -> warehouse CAN_USE", "ERROR", str(e))

def grant_genie(sp: str, who: str):
    if not GENIE_SPACE_ID:
        record(f"{who} -> Genie space CAN_RUN", "WARN", "no genie_space_id; grant CAN RUN in the Genie UI")
        return
    try:
        w.api_client.do("PATCH", f"/api/2.0/permissions/genie/{GENIE_SPACE_ID}",
                        body={"access_control_list": [
                            {"service_principal_name": sp, "permission_level": "CAN_RUN"}]})
        record(f"{who} -> Genie space CAN_RUN", "OK", GENIE_SPACE_ID)
    except Exception as e:
        record(f"{who} -> Genie space CAN_RUN", "WARN",
               f"couldn't set via API ({e}); grant CAN RUN to {sp} on the space in the Genie UI")

# Manager = FULL data access.
if MANAGER_SP:
    grant_warehouse(MANAGER_SP, "manager")
    grant_genie(MANAGER_SP, "manager")
    grant_sql(f"GRANT USE CATALOG, USE SCHEMA, SELECT, EXECUTE ON CATALOG {CATALOG} TO `{MANAGER_SP}`",
              "manager -> catalog (full)")

# Analyst = RESTRICTED: only sales_daily + market_share + f_market_share (NO promo).
# References only objects that exist in every variant, so it's safe for Customer B too.
if ANALYST_SP:
    grant_warehouse(ANALYST_SP, "analyst")
    grant_genie(ANALYST_SP, "analyst")
    grant_sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{ANALYST_SP}`", "analyst -> USE CATALOG")
    grant_sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO `{ANALYST_SP}`", "analyst -> USE SCHEMA gold")
    grant_sql(f"GRANT SELECT ON TABLE {CATALOG}.gold.sales_daily TO `{ANALYST_SP}`", "analyst -> SELECT sales_daily")
    grant_sql(f"GRANT SELECT ON TABLE {CATALOG}.gold.market_share TO `{ANALYST_SP}`", "analyst -> SELECT market_share")
    grant_sql(f"GRANT EXECUTE ON FUNCTION {CATALOG}.gold.f_market_share TO `{ANALYST_SP}`", "analyst -> EXECUTE f_market_share")
    # NOTE: intentionally NOT granting promo_performance / f_promo_lift to the analyst.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(_results)[["step", "status", "detail"]]
order = {"ERROR": 0, "WARN": 1, "OK": 2}
df = df.sort_values(by="status", key=lambda s: s.map(order)).reset_index(drop=True)
n_err = (df["status"] == "ERROR").sum()
print(f"app SP={APP_SP} | analyst={ANALYST_SP} | manager={MANAGER_SP}")
print("ALL GRANTS APPLIED" if n_err == 0 else f"{n_err} grant(s) FAILED — see rows")
print("Reminder: analyst is intentionally NOT granted promo access (enforcement demo).")
display(df)
