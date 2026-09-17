# Databricks notebook source
# MAGIC %md
# MAGIC # Cerebro Test Kitchen — Pre-Workshop Preflight
# MAGIC
# MAGIC Run this **once, before the workshop**, in the **customer's training workspace**,
# MAGIC with someone from Cerebro's side. Run it **as a workspace admin** (group + service
# MAGIC principal creation may additionally need **account admin** — the notebook tells you
# MAGIC if it hits that wall).
# MAGIC
# MAGIC ### Goal
# MAGIC Get all **enablements, the participants group, catalogs + grants, the warehouse
# MAGIC grant, the secret scope, and the persona service principals** ready — so the
# MAGIC **day-of is pure hands-on**: the team practices deploying the baseline, creating
# MAGIC the Genie space, wiring the Unity AI Gateway, seeding data, and deploying the app.
# MAGIC
# MAGIC ### What this notebook does
# MAGIC - **Checks** (read-only): serverless compute, UC metastore, Unity AI Gateway + Claude,
# MAGIC   Genie (+ Genie Code), Lakebase, Databricks Apps enablement.
# MAGIC - **Creates** (mutating): participants group (optional), the 4 catalogs + grants,
# MAGIC   warehouse `CAN_USE` grant, the secret scope, the 2 persona service principals.
# MAGIC - **Prints** the values you'll paste into `databricks.yml` and
# MAGIC   `src/app/config/*.yaml` on the day.
# MAGIC
# MAGIC ### What this notebook deliberately does NOT do (day-of practice)
# MAGIC `bundle deploy` · `apply_ddl` · `seed_data` · `run_pipeline` · Genie space creation ·
# MAGIC Unity AI Gateway model-service creation · app deployment. Those are the hands-on
# MAGIC exercises for the workshop.
# MAGIC
# MAGIC > Set **`do_mutations = false`** first for a **dry-run** (checks only), review the
# MAGIC > summary, then re-run with `true` to actually create things.

# COMMAND ----------

dbutils.widgets.text("participants_group", "cerebro-workshop", "1. Participants group name")
dbutils.widgets.text("warehouse_id", "", "2. Serverless SQL warehouse ID")
dbutils.widgets.text("catalogs", "cerebro_dev,cerebro_a,cerebro_b,cerebro_c", "3. Catalogs (comma-sep)")
dbutils.widgets.text("secret_scope", "cerebro_demo", "4. Secret scope name")
dbutils.widgets.text("claude_model", "system.ai.claude-sonnet-4-5", "5. Claude model FQN (Unity AI Gateway)")
dbutils.widgets.dropdown("do_mutations", "false", ["false", "true"], "6. Actually create things? (false = dry-run)")
dbutils.widgets.dropdown("create_group_if_missing", "true", ["true", "false"], "7. Create group if missing")
dbutils.widgets.dropdown("create_personas", "true", ["true", "false"], "8. Create persona SPs")

GROUP        = dbutils.widgets.get("participants_group").strip()
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id").strip()
CATALOGS     = [c.strip() for c in dbutils.widgets.get("catalogs").split(",") if c.strip()]
SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
CLAUDE_MODEL = dbutils.widgets.get("claude_model").strip()
DO           = dbutils.widgets.get("do_mutations") == "true"
CREATE_GROUP = dbutils.widgets.get("create_group_if_missing") == "true"
CREATE_SPS   = dbutils.widgets.get("create_personas") == "true"

# Persona service principals -> secret-scope keys (must match src/app/auth.py)
PERSONAS = {
    "cerebro-persona-analyst": ("persona_a_client_id", "persona_a_client_secret"),
    "cerebro-persona-manager": ("persona_b_client_id", "persona_b_client_secret"),
}

PRIVS = ("USE CATALOG, USE SCHEMA, CREATE SCHEMA, CREATE TABLE, "
         "CREATE FUNCTION, CREATE VOLUME, SELECT, EXECUTE, MODIFY")

print(f"Mode: {'MUTATING (will create)' if DO else 'DRY-RUN (checks only)'}")
print(f"Group={GROUP} | Warehouse={WAREHOUSE_ID or '(none provided)'} | Scope={SECRET_SCOPE}")
print(f"Catalogs={CATALOGS} | Claude model={CLAUDE_MODEL}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me()
HOST = w.config.host
print("Running as:", me.user_name)
print("Workspace host (this is your databricks.yml `workspace.host` for every target):")
print("   ", HOST)

# --- results collector -------------------------------------------------------
# status: OK (done/verified) | ACTION (you must do something) | WARN (verify manually) | SKIP
_results = []
def record(step, status, detail=""):
    _results.append({"step": step, "status": status, "detail": str(detail)[:300]})
    print(f"[{status:6}] {step} — {detail}")

def do_or_note(step, fn, manual_hint):
    """Run a mutating action only when DO=true; capture failures as ACTION items."""
    if not DO:
        record(step, "SKIP", "dry-run — will do on next run with do_mutations=true")
        return
    try:
        msg = fn()
        record(step, "OK", msg or "done")
    except Exception as e:
        record(step, "ACTION", f"could not auto-do ({type(e).__name__}: {str(e)[:120]}). Manual: {manual_hint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1 — Enablement checks (read-only)
# MAGIC Confirms the platform features the workshop depends on. These are *checks*; where a
# MAGIC check can't be made programmatically, it's flagged WARN with how to verify in the UI.

# COMMAND ----------

# --- UC metastore attached ---
try:
    cats = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
    record("UC metastore attached", "OK", f"{len(cats)} catalogs visible")
except Exception as e:
    record("UC metastore attached", "ACTION", f"SHOW CATALOGS failed: {e}. Attach a UC metastore to this workspace.")

# --- CREATE CATALOG privilege (the FEVM wall) ---
if DO:
    _probe = f"__cerebro_preflight_probe"
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {_probe}")
        spark.sql(f"DROP CATALOG IF EXISTS {_probe}")
        record("CREATE CATALOG privilege", "OK", "you can create catalogs")
    except Exception as e:
        record("CREATE CATALOG privilege", "ACTION",
               f"no CREATE CATALOG ({str(e)[:100]}). Need metastore admin / a CREATE CATALOG grant on the metastore.")
else:
    record("CREATE CATALOG privilege", "SKIP", "dry-run — re-run with do_mutations=true to probe")

# --- Serverless compute (proxy: is a serverless SQL warehouse available?) ---
try:
    whs = list(w.warehouses.list())
    serverless = [x for x in whs if getattr(x, "enable_serverless_compute", False)]
    record("Serverless SQL warehouse available", "OK" if serverless else "WARN",
           f"{len(serverless)} serverless of {len(whs)} warehouses. "
           + ("" if serverless else "Create a serverless warehouse; also confirm serverless jobs/notebooks entitlement in Admin settings."))
except Exception as e:
    record("Serverless SQL warehouse available", "WARN", f"couldn't list warehouses: {e}")

# --- Unity AI Gateway + Claude access ---
try:
    eps = [e.name for e in w.serving_endpoints.list()]
    claude = [n for n in eps if "claude-sonnet-4-5" in n or "claude-sonnet-5" in n]
    record("Unity AI Gateway — Claude available", "OK" if claude else "WARN",
           f"found {claude[:3]}. Gateway FQN to use: {CLAUDE_MODEL} (query via /ai-gateway/mlflow/v1/chat/completions). "
           "Verify the participants group AND the persona SPs can query it (Catalog Explorer > system > ai > the model > Permissions, or the endpoint's CAN QUERY).")
except Exception as e:
    record("Unity AI Gateway — Claude available", "WARN", f"couldn't list serving endpoints: {e}")

# --- Genie + Genie Code ---
try:
    # Best-effort: the Genie API surface varies by SDK version.
    ok = hasattr(w, "genie")
    record("Genie enabled (+ Genie Code)", "WARN",
           "SDK has genie API" if ok else "verify in UI: Genie/Genie spaces are enabled, "
           "participants can CREATE spaces, and the Genie Code agent is available to them.")
except Exception as e:
    record("Genie enabled (+ Genie Code)", "WARN", f"verify manually in the UI. ({e})")

# --- Lakebase enabled ---
try:
    _ = list(w.database.list_database_instances())  # newer SDK
    record("Lakebase enabled", "OK", "database instances API reachable; confirm the group can create an instance")
except Exception as e:
    record("Lakebase enabled", "WARN",
           f"couldn't auto-check ({type(e).__name__}). Verify in UI: Lakebase enabled + group can create a database instance.")

# --- Databricks Apps enabled ---
try:
    _ = list(w.apps.list())
    record("Databricks Apps enabled", "OK", "apps API reachable; confirm the group can create an app")
except Exception as e:
    record("Databricks Apps enabled", "WARN",
           f"couldn't auto-check ({type(e).__name__}). Verify in UI: Apps enabled + group can create an app.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2 — Provisioning (mutating; gated by `do_mutations`)
# MAGIC Participants group, the 4 catalogs + grants, warehouse `CAN_USE`, the secret scope,
# MAGIC and the persona service principals.

# COMMAND ----------

# --- 2a. Participants group ---
try:
    existing = list(w.groups.list(filter=f'displayName eq "{GROUP}"'))
    if existing:
        record("Participants group exists", "OK", f'"{GROUP}" (id {existing[0].id})')
    elif CREATE_GROUP:
        do_or_note(f'Create group "{GROUP}"',
                   lambda: (w.groups.create(display_name=GROUP), f'created "{GROUP}"')[1],
                   f'create the group "{GROUP}" and add participants (Admin > Groups)')
    else:
        record("Participants group exists", "ACTION", f'"{GROUP}" not found and create_group_if_missing=false')
except Exception as e:
    record("Participants group", "WARN", f"couldn't check groups ({e}); verify/create manually")

# COMMAND ----------

# --- 2b. Catalogs + grants to the participants group ---
for cat in CATALOGS:
    do_or_note(
        f"Catalog {cat} + grants",
        (lambda c=cat: (
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {c}"),
            spark.sql(f"GRANT {PRIVS} ON CATALOG {c} TO `{GROUP}`"),
            f"created + granted [{PRIVS}] to {GROUP}",
        )[-1]),
        manual_hint=f"CREATE CATALOG {cat}; GRANT {PRIVS} ON CATALOG {cat} TO `{GROUP}` (needs metastore admin/CREATE CATALOG)",
    )

# COMMAND ----------

# --- 2c. Warehouse CAN_USE to the participants group ---
if not WAREHOUSE_ID:
    record("Warehouse CAN_USE grant", "ACTION", "no warehouse_id provided — create a serverless SQL warehouse and set the widget")
else:
    def _grant_wh():
        w.warehouses.get(WAREHOUSE_ID)  # verify it exists
        w.api_client.do(
            "PATCH", f"/api/2.0/permissions/warehouses/{WAREHOUSE_ID}",
            body={"access_control_list": [{"group_name": GROUP, "permission_level": "CAN_USE"}]},
        )
        return f"CAN_USE on warehouse {WAREHOUSE_ID} granted to {GROUP}"
    do_or_note("Warehouse CAN_USE grant", _grant_wh,
               manual_hint=f"SQL Warehouses > {WAREHOUSE_ID} > Permissions > add {GROUP} as CAN USE")

# COMMAND ----------

# --- 2d. Secret scope ---
def _make_scope():
    scopes = [s.name for s in w.secrets.list_scopes()]
    if SECRET_SCOPE in scopes:
        return f'scope "{SECRET_SCOPE}" already exists'
    w.secrets.create_scope(scope=SECRET_SCOPE)
    return f'created scope "{SECRET_SCOPE}"'
do_or_note(f'Secret scope "{SECRET_SCOPE}"', _make_scope,
           manual_hint=f'databricks secrets create-scope {SECRET_SCOPE}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2e. Persona service principals + OAuth secrets
# MAGIC Creating an SP is workspace-level; **generating its OAuth secret is usually
# MAGIC account-admin**. The notebook creates the SPs and tries the secret; if it can't,
# MAGIC it prints exactly what to create in the account console and gives you a ready cell
# MAGIC (below) to store the creds in the scope.

# COMMAND ----------

sp_ids = {}
if CREATE_SPS:
    for name in PERSONAS:
        def _make_sp(n=name):
            found = list(w.service_principals.list(filter=f'displayName eq "{n}"'))
            if found:
                sp_ids[n] = found[0].application_id
                return f'exists (app_id {found[0].application_id})'
            sp = w.service_principals.create(display_name=n)
            sp_ids[n] = sp.application_id
            return f'created (app_id {sp.application_id})'
        do_or_note(f"Persona SP {name}", _make_sp,
                   manual_hint=f'create service principal "{name}" (Admin > Service principals)')
    # OAuth secrets — best effort; account-admin API. On failure, guided-manual.
    record("Persona OAuth secrets",
           "WARN" if DO else "SKIP",
           "OAuth-secret generation is account-level and often not available from a workspace notebook. "
           "Create a secret for each SP in the Account console (Service principals > the SP > Generate secret), "
           "then run the '2f' storage cell below with the client_id/client_secret values.")
else:
    record("Persona SPs", "SKIP", "create_personas=false")

print("Persona app_ids so far:", sp_ids)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2f. Store persona OAuth creds in the secret scope
# MAGIC Fill the four values (from the SP secrets you generated) and run. Keys match
# MAGIC `src/app/auth.py`. Leave blank to skip.

# COMMAND ----------

PERSONA_A_CLIENT_ID     = ""   # cerebro-persona-analyst application (client) id
PERSONA_A_CLIENT_SECRET = ""   # cerebro-persona-analyst OAuth secret
PERSONA_B_CLIENT_ID     = ""   # cerebro-persona-manager application (client) id
PERSONA_B_CLIENT_SECRET = ""   # cerebro-persona-manager OAuth secret

_pairs = {
    "persona_a_client_id": PERSONA_A_CLIENT_ID,
    "persona_a_client_secret": PERSONA_A_CLIENT_SECRET,
    "persona_b_client_id": PERSONA_B_CLIENT_ID,
    "persona_b_client_secret": PERSONA_B_CLIENT_SECRET,
}
if all(v for v in _pairs.values()):
    for k, v in _pairs.items():
        w.secrets.put_secret(scope=SECRET_SCOPE, key=k, string_value=v)
    record("Persona creds stored in scope", "OK", f"put {list(_pairs)} into {SECRET_SCOPE}")
else:
    record("Persona creds stored in scope", "ACTION",
           "fill PERSONA_*_CLIENT_ID/SECRET above (after generating SP secrets) and re-run this cell")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3 — Values for the day (paste these on the day, don't deploy now)

# COMMAND ----------

print("Paste into databricks.yml — every target's workspace.host:")
print(f"   host: {HOST}")
print(f"\nPaste into databricks.yml — variables.warehouse_id.default:")
print(f"   {WAREHOUSE_ID or '(create a serverless SQL warehouse and use its ID)'}")
print(f"\nPaste into each src/app/config/<customer>.yaml — claude_model:")
print(f"   {CLAUDE_MODEL}")
print("\nStill DAY-OF (team does these live — do NOT do now):")
for x in ["apply_ddl", "seed_data", "run_pipeline", "register_functions",
          "create the Genie space (genie/genie_space.md)",
          "create/point at the Unity AI Gateway model",
          "bundle deploy the app + grant the app SP (secret scope + catalog + Lakebase)"]:
    print("   -", x)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4 — Summary

# COMMAND ----------

import pandas as pd
summary = pd.DataFrame(_results)[["step", "status", "detail"]]
order = {"ACTION": 0, "FAIL": 1, "WARN": 2, "SKIP": 3, "OK": 4}
summary = summary.sort_values(by="status", key=lambda s: s.map(order)).reset_index(drop=True)
n_action = (summary["status"] == "ACTION").sum()
n_ok = (summary["status"] == "OK").sum()
print(f"OK={n_ok}  ACTION={n_action}  (review WARN/SKIP rows too)")
print("READY for the workshop" if (n_action == 0 and DO) else
      "Resolve ACTION rows (and re-run mutating pass if this was a dry-run) before the day.")
display(summary)
