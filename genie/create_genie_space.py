# Databricks notebook source
# MAGIC %md
# MAGIC # Create the Cerebro Genie space (genie-as-code)
# MAGIC
# MAGIC Creates (or finds) the **Cerebro Market Analytics** Genie space over
# MAGIC `${catalog}.gold`, programmatically via the Genie API
# MAGIC (`WorkspaceClient.genie.create_space` → `POST /api/2.0/genie/spaces`). Backs
# MAGIC the `create_genie_space` DABs job, so `databricks bundle run create_genie_space
# MAGIC -t <target>` builds the space for that customer's catalog.
# MAGIC
# MAGIC **It reads the buildable contract in `genie/genie_space.md`** for the sample
# MAGIC questions and the general-instruction block, and uses a per-customer table map
# MAGIC (A/C/dev = sales_daily + market_share + promo_performance; B = sales_daily +
# MAGIC market_share + omnichannel_sales).
# MAGIC
# MAGIC ## Prerequisites (order matters)
# MAGIC Run **after** the gold tables exist and are populated — the create call
# MAGIC validates the referenced Unity Catalog tables:
# MAGIC `apply_ddl → seed_data → run_pipeline` (and `register_functions`) first.
# MAGIC
# MAGIC ## Output
# MAGIC Prints the **`space_id`** — copy it into `src/app/config/<customer>.yaml`
# MAGIC (`genie_space_id`). Also set as a job task value `genie_space_id`.
# MAGIC
# MAGIC > Advanced curation (trusted-asset functions, synonyms/entity matching, join
# MAGIC > specs, benchmark Q&A) is layered on top afterward — in the Genie UI or via
# MAGIC > export→edit→import — per `genie_space.md`. This notebook creates the space
# MAGIC > with its tables, sample questions, and instruction context; that's the
# MAGIC > reliable, API-supported baseline.

# COMMAND ----------

dbutils.widgets.text("catalog", "cerebro_dev", "Target catalog")
dbutils.widgets.dropdown("customer_slug", "dev", ["dev", "a", "b", "c"], "Customer slug")
dbutils.widgets.text("warehouse_id", "", "Serverless SQL warehouse ID (required)")
dbutils.widgets.text("parent_path", "", "Workspace folder for the space (blank = your home)")
dbutils.widgets.dropdown("recreate_if_exists", "false", ["false", "true"], "Replace if a space with the same title exists")

CATALOG   = dbutils.widgets.get("catalog").strip()
SLUG      = dbutils.widgets.get("customer_slug").strip().lower()
WAREHOUSE = dbutils.widgets.get("warehouse_id").strip()
PARENT    = dbutils.widgets.get("parent_path").strip()
RECREATE  = dbutils.widgets.get("recreate_if_exists") == "true"

IS_B  = SLUG == "b"
TITLE = f"Cerebro Market Analytics — {CATALOG}"

# Per-customer table set (CONTRACT.md ground truth). B swaps promo_performance for
# omnichannel_sales; dev mirrors A.
TABLES = ["sales_daily", "market_share", "omnichannel_sales" if IS_B else "promo_performance"]
TABLE_IDENTIFIERS = [f"{CATALOG}.gold.{t}" for t in TABLES]

print(f"catalog={CATALOG} slug={SLUG} is_b={IS_B}")
print("tables:", TABLE_IDENTIFIERS)
assert WAREHOUSE, "warehouse_id is required (Genie needs a serverless SQL warehouse)."

# COMMAND ----------

import json
import os
import re
import uuid

import requests
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me().user_name
if not PARENT:
    PARENT = f"/Users/{me}"
print("running as:", me, "| parent_path:", PARENT)

# COMMAND ----------

# --- Locate + read genie/genie_space.md (the buildable contract) --------------
def find_repo_root() -> str:
    starts = []
    try:
        starts.append(os.path.dirname(os.path.abspath(__file__)))
    except NameError:
        pass
    starts.append(os.getcwd())
    for start in starts:
        d = start
        for _ in range(8):
            if os.path.exists(os.path.join(d, "databricks.yml")):
                return d
            parent = os.path.dirname(d)
            if parent == d:
                break
            d = parent
    return os.getcwd()

MD_PATH = os.path.join(find_repo_root(), "genie", "genie_space.md")
try:
    with open(MD_PATH) as f:
        MD = f.read()
    print("loaded contract:", MD_PATH)
except Exception as e:
    MD = ""
    print(f"WARN: could not read genie_space.md ({e}); using built-in fallbacks.")

# COMMAND ----------

# --- Parse sample questions from the md, filtered for this customer -----------
# Falls back to a small built-in set if parsing finds nothing.
_FALLBACK_QUESTIONS = [
    "What was total revenue last quarter by category?",
    "Which brand has the highest market share in the Northeast?",
    "What are the top 5 brands by revenue this year?",
    "How did units sold trend month over month?",
]

def parse_sample_questions(md: str, is_b: bool) -> list:
    if not md:
        return _FALLBACK_QUESTIONS
    # grab the "## ~10 sample questions" section
    m = re.search(r"##[^\n]*sample questions(.*?)(?:\n##\s|\Z)", md, re.S | re.I)
    if not m:
        return _FALLBACK_QUESTIONS
    out = []
    for line in m.group(1).splitlines():
        line = line.strip()
        mm = re.match(r"^\d+\.\s+(.*)$", line)
        if not mm:
            continue
        text = mm.group(1)
        # tag-based filtering: [B] only for B; [A/C] only for non-B
        low = text.lower()
        is_b_only = "[b]" in low
        is_ac_only = ("[a/c]" in low) and not is_b_only
        if is_b_only and not is_b:
            continue
        if is_ac_only and is_b:
            continue
        # strip tags / markdown emphasis / trailing "(A/C/B)"
        text = re.sub(r"\*\*\[[^\]]*\]\*\*", "", text)
        text = re.sub(r"\*\([^)]*\)\*", "", text)
        text = text.replace("**", "").strip()
        if text:
            out.append(text)
    return out or _FALLBACK_QUESTIONS

SAMPLE_QUESTIONS = parse_sample_questions(MD, IS_B)
print(f"{len(SAMPLE_QUESTIONS)} sample questions")
for q in SAMPLE_QUESTIONS:
    print("  -", q)

# COMMAND ----------

# --- Parse the general-instruction block for the description ------------------
def parse_general_instructions(md: str, catalog: str, is_b: bool) -> str:
    base = (f"Cerebro market-analytics assistant over {catalog}.gold "
            "(bakery franchise). Currency USD; round revenue to whole dollars and "
            "share/lift to one decimal. 'Market share' = share_pct from market_share; "
            "prefer the certified f_market_share / f_promo_lift functions when present.")
    if not md:
        extra = (" Sales are omnichannel (in-store + online); break out by channel. "
                 "No promotions data — promo lift is not tracked."
                 if is_b else "")
        return base + extra
    m = re.search(r"##\s*General instructions.*?\n(.*?)(?:\n##\s|\Z)", md, re.S | re.I)
    block = ""
    if m:
        lines = [re.sub(r"^\s*>\s?", "", ln) for ln in m.group(1).splitlines() if ln.strip().startswith(">")]
        block = " ".join(lines).strip()
    if not block:
        return base
    # keep only the customer-relevant conditional notes
    sentences = re.split(r"(?<=[.])\s+", block)
    keep = []
    for s in sentences:
        low = s.lower()
        if "[customer c only]" in low:
            if SLUG == "c":
                keep.append(re.sub(r"\*\*\[[^\]]*\]\*\*", "", s).strip())
            continue
        if "[customer b only]" in low:
            if is_b:
                keep.append(re.sub(r"\*\*\[[^\]]*\]\*\*", "", s).strip())
            continue
        keep.append(s.strip())
    return re.sub(r"\s+", " ", " ".join(keep)).strip() or base

DESCRIPTION = parse_general_instructions(MD, CATALOG, IS_B)
print("description:\n ", DESCRIPTION)

# COMMAND ----------

# --- Build the serialized_space (v2) — documented reliable minimum ------------
# version + data_sources.tables[].identifier + config.sample_questions.
# Richer curation (trusted functions, synonyms, join specs, benchmarks) is layered
# afterward per genie_space.md — those nested shapes aren't part of this baseline.
# serialized_space (v2) — sent as a single-encoded JSON STRING in the request body
# (json.dumps below). Validation rules per the docs
# (.../genie-agents/conversation-api#validation-rules-for-serialized_space):
#   - EVERY `id` = 32-char LOWERCASE HEX (a UUID with hyphens removed) -> uuid4().hex
#   - config.sample_questions[] = {"id", "question": [<str>]}, SORTED BY id
#   - data_sources.tables[]     = {"identifier": "catalog.schema.table"}, SORTED BY identifier
# Baseline is intentionally minimal (tables + sample questions). Richer instructions
# / benchmarks (each element also needs its own hex id + per-collection sort) are
# layered later per genie_space.md.
_sample_qs = sorted(
    ({"id": uuid.uuid4().hex, "question": [q]} for q in SAMPLE_QUESTIONS),
    key=lambda e: e["id"],
)
serialized_space = {
    "version": 2,
    "config": {"sample_questions": _sample_qs},
    "data_sources": {
        "tables": [{"identifier": ident} for ident in sorted(TABLE_IDENTIFIERS)]
    },
}
print(json.dumps(serialized_space, indent=2))

# COMMAND ----------

# --- Idempotent create via the REST API -------------------------------------
# The SDK's genie.create_space isn't present on every SDK version, so we call the
# REST endpoint directly through w.api_client.do — version-independent.
#   list:   GET    /api/2.0/genie/spaces         -> {"spaces":[...], "next_page_token"}
#   create: POST   /api/2.0/genie/spaces         -> {"space_id": ...}
#   delete: DELETE /api/2.0/genie/spaces/{id}
def _api(method, path, body=None):
    return w.api_client.do(method, path, body=body) or {}

def _space_name(s: dict):
    return s.get("title") or s.get("display_name") or s.get("name")

def find_existing_space_id(title: str):
    try:
        token, seen = None, 0
        while True:
            path = "/api/2.0/genie/spaces" + (f"?page_token={token}" if token else "")
            resp = _api("GET", path)
            for s in (resp.get("spaces") or []):
                if _space_name(s) == title:
                    return s.get("space_id") or s.get("id")
            token = resp.get("next_page_token")
            seen += 1
            if not token or seen > 20:
                break
    except Exception as e:
        print(f"(could not list existing spaces: {e})")
    return None

def create_space():
    # `serialized_space` is a STRING field whose CONTENT must be a JSON object,
    # single-encoded on the wire. We build the request with `requests` (not
    # api_client.do, which re-encoded the value and produced a double-encoded
    # string). requests' json= serializes the body once, so serialized_space ends
    # up as one JSON string literal containing the object — exactly what the API
    # wants ("String field" + valid-JSON-object content).
    host = w.config.host.rstrip("/")
    headers = {**(w.config.authenticate() or {}), "Content-Type": "application/json"}

    def _post(include_parent: bool):
        body = {
            "warehouse_id": WAREHOUSE,
            "title": TITLE,
            "description": DESCRIPTION,
            "serialized_space": json.dumps(serialized_space),   # single-encoded JSON string
        }
        if include_parent and PARENT:
            body["parent_path"] = PARENT
        r = requests.post(f"{host}/api/2.0/genie/spaces", headers=headers, json=body, timeout=90)
        if not r.ok:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:500]}")
        return r.json()

    try:
        return _post(include_parent=True)
    except Exception as e:
        # only retry without parent_path if THAT is what the API complained about
        if PARENT and "parent_path" in str(e).lower():
            print(f"(parent_path rejected: {e}; retrying without it)")
            return _post(include_parent=False)
        raise

existing_id = find_existing_space_id(TITLE)

if existing_id and not RECREATE:
    space_id = existing_id
    print(f"Space already exists (idempotent): {space_id}. "
          "Set recreate_if_exists=true to replace it.")
else:
    if existing_id and RECREATE:
        try:
            _api("DELETE", f"/api/2.0/genie/spaces/{existing_id}")
            print(f"deleted existing space {existing_id}")
        except Exception as e:
            print(f"WARN: could not delete existing space ({e}); creating a new one.")
    resp = create_space()
    space_id = resp.get("space_id") or resp.get("id") or (resp.get("space") or {}).get("space_id")
    print(f"Created Genie space: {space_id}")
    if not space_id:
        print("WARN: create returned no space_id; full response:", json.dumps(resp)[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result — copy this into your config

# COMMAND ----------

print("=" * 66)
print(f"  Genie space id : {space_id}")
print(f"  Title          : {TITLE}")
print(f"  Catalog/tables : {TABLE_IDENTIFIERS}")
print("-" * 66)
print(f"  Paste into src/app/config/customer_{SLUG}.yaml (or dev.yaml):")
print(f"      genie_space_id: \"{space_id}\"")
print("  Then set claude_model: system.ai.claude-sonnet-4-5 and re-deploy the app.")
print("  Next: enrich the space in the Genie UI per genie/genie_space.md")
print("        (trusted functions f_market_share/f_promo_lift, synonyms, benchmarks).")
print("=" * 66)

# Expose to downstream job tasks (best-effort).
try:
    dbutils.jobs.taskValues.set(key="genie_space_id", value=str(space_id))
except Exception:
    pass
