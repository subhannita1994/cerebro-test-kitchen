"""Runtime customer-config resolver — the config-injection mechanism.

WHY this exists (deploy contract): Databricks Apps read env from app.yaml as
STATIC values — the bundle does NOT substitute `${var.*}` inside app.yaml — and
only the `src/app/` folder is deployed, so repo-root files aren't reachable at
runtime. So per-customer behavior can't ride on `${var.*}` env or an external
config path. Instead we BUNDLE one yaml per target under `src/app/config/` and
resolve the active one AT RUNTIME from the app's own name.

Resolution order:
  1. `CEREBRO_CUSTOMER` env (local-dev override) — a slug in {dev, a, b, c}.
  2. `DATABRICKS_APP_NAME` (auto-injected by the Apps runtime), format
     `cerebro-assistant-<slug>` where slug ∈ {dev, a, b, c}.
  3. Fall back to dev.yaml with a logged warning if unresolved.

The whole app (agent, tools, state, tracing) reads catalog / claude_model /
genie_space_id / enable_lakebase_serving / tools / lakebase_* from here, so the
SAME image deployed as cerebro-assistant-a vs -b vs -c behaves differently.
"""
from __future__ import annotations

import functools
import logging
import os

import yaml

log = logging.getLogger("cerebro.config")

_CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config")
_APP_PREFIX = "cerebro-assistant-"

# slug -> bundled config file
_SLUG_TO_FILE = {
    "dev": "dev.yaml",
    "a": "customer_a.yaml",
    "b": "customer_b.yaml",
    "c": "customer_c.yaml",
}

# Tools that require Lakebase serving; gated off unless enable_lakebase_serving.
# (Mirrors the `lakebase=True` specs in tools.py; kept as a plain set here to
# avoid a customer_config -> tools -> state -> customer_config import cycle.)
_LAKEBASE_TOOLS = {"get_top_movers", "save_to_watchlist"}


def _normalize_slug(raw: str) -> str:
    """Accept a bare slug (dev/a/b/c) or a full stem (customer_a) or app name."""
    s = (raw or "").strip().lower()
    if s.startswith(_APP_PREFIX):
        s = s[len(_APP_PREFIX):]
    if s.startswith("customer_"):
        s = s[len("customer_"):]
    return s


def _resolve_slug() -> str:
    override = os.environ.get("CEREBRO_CUSTOMER")
    if override:
        slug = _normalize_slug(override)
        if slug in _SLUG_TO_FILE:
            return slug
        log.warning("CEREBRO_CUSTOMER=%r did not resolve to a known slug; continuing to app-name", override)

    app_name = os.environ.get("DATABRICKS_APP_NAME", "")
    if app_name.startswith(_APP_PREFIX):
        slug = _normalize_slug(app_name)
        if slug in _SLUG_TO_FILE:
            return slug

    log.warning("Could not resolve customer (CEREBRO_CUSTOMER=%r, DATABRICKS_APP_NAME=%r); "
                "defaulting to dev.", os.environ.get("CEREBRO_CUSTOMER"), app_name)
    return "dev"


@functools.lru_cache(maxsize=1)
def _load() -> dict:
    slug = _resolve_slug()
    fname = _SLUG_TO_FILE.get(slug, "dev.yaml")
    path = os.path.join(_CONFIG_DIR, fname)
    with open(path) as f:
        cfg = yaml.safe_load(f) or {}
    cfg["_slug"] = slug
    log.info("customer config resolved: slug=%s file=%s catalog=%s serving=%s",
             slug, fname, cfg.get("catalog"), cfg.get("enable_lakebase_serving"))
    return cfg


# --- accessors --------------------------------------------------------------
def slug() -> str:
    return _load()["_slug"]


def customer() -> str:
    return _load().get("customer", slug().upper())


def display_name() -> str:
    return _load().get("display_name", customer())


def catalog() -> str:
    return _load()["catalog"]


def claude_model() -> str:
    return _load()["claude_model"]


def genie_space_id() -> str:
    return _load()["genie_space_id"]


def enable_lakebase_serving() -> bool:
    return bool(_load().get("enable_lakebase_serving", False))


def lakebase_instance() -> str:
    return _load().get("lakebase_instance", "cerebro-lakebase")


def lakebase_host() -> str:
    return _load().get("lakebase_host", "REPLACE_ME")


def lakebase_database() -> str:
    return _load().get("lakebase_database", "databricks_postgres")


def enabled_tools() -> list[str]:
    """The tools for this customer. Belt-and-suspenders: drop the Lakebase tools
    unless serving is enabled, and always keep query_genie."""
    tools = list(_load().get("tools") or [])
    if not enable_lakebase_serving():
        tools = [t for t in tools if t not in _LAKEBASE_TOOLS]
    if "query_genie" not in tools:
        tools = ["query_genie", *tools]
    return tools
