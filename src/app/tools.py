"""Config-driven tool registry — the heart of the workshop lesson.

Tool AVAILABILITY is per-customer CONFIG, not a code fork. The SAME app image
deploys to catalogs cerebro_a / cerebro_b / cerebro_c; the only thing that
changes is which tools we hand Claude, and that list comes from the customer's
`config/customer_*.yaml` (surfaced to the app as the TOOLS_CONFIG env var, which
the DAB fills from `${var.tools_config}`).

Each tool = an OpenAI-style function schema (what Claude sees) + an executor
(what actually runs) + a friendly "notice" the UI shows while it runs.

Data-access boundary (unchanged from the reference app):
  * `query_genie` and the UC-function tools run AS THE PERSONA service principal
    (persona_token), so Unity Catalog enforces per-persona access on every call.
  * The Lakebase tools run through state.py, which connects AS THE APP service
    principal (the Postgres table owner) — reads from the synced snapshot and
    writes to the OLTP watchlist.

`build_tools(enabled)` returns (tool_schemas, executor_map) for ONLY the enabled
tools, so A/B/C behave differently from identical code.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import requests

import state
from genie import GenieClient, GenieResult


# ---------------------------------------------------------------------------
# UI event — a single streamed step for the Streamlit UI. Defined here (not in
# agent.py) so both the agent loop and the tool executors can build events
# without a circular import. agent.py re-exports this.
# ---------------------------------------------------------------------------
@dataclass
class Event:
    """A streamed step for the UI.

    type ∈ {"reasoning", "tool_notice", "genie_step", "tool_step", "answer", "error"}
    """
    type: str
    text: Optional[str] = None      # reasoning delta, notice text, or final answer
    genie_step: Any = None          # GenieStep when type == "genie_step"
    data: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Tool execution context + result. The context carries everything an executor
# needs: the persona identity/token (for UC + Genie), the target catalog +
# warehouse (for UC-function SQL), and a live Genie client. `genie_conversation_id`
# is mutable so the query_genie tool can thread Genie's own memory across turns.
# ---------------------------------------------------------------------------
@dataclass
class ToolContext:
    host: str
    persona_token: str                      # persona SP OAuth token
    persona_key: str
    username: str
    catalog: str
    warehouse_id: str
    genie: GenieClient
    lakebase_enabled: bool = False
    genie_conversation_id: Optional[str] = None


@dataclass
class ToolResult:
    """What an executor returns.

    llm_content — compact string fed back to Claude as the tool result.
    events      — UI Events to yield (genie_step / tool_step).
    meta        — updates the agent merges into the TurnLog + persisted steps.
    """
    llm_content: str
    events: list = field(default_factory=list)
    meta: dict = field(default_factory=dict)


Executor = Callable[[ToolContext, dict], ToolResult]


@dataclass
class ToolSpec:
    schema: dict            # OpenAI-style function schema sent to Claude
    executor: Executor      # (ctx, args) -> ToolResult
    notice: str             # shown in the UI BEFORE the (possibly slow) call
    summary: str            # one-liner used to build the system prompt
    lakebase: bool = False  # gated on ENABLE_LAKEBASE_SERVING


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _fn_schema(name: str, description: str, properties: dict, required: list[str]) -> dict:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {"type": "object", "properties": properties, "required": required},
        },
    }


def _execute_sql(ctx: ToolContext, statement: str, params: list[dict]) -> tuple[list[str], list[list[Any]]]:
    """Run a parameterized statement on the persona SP via the SQL Statement
    Execution API. NAMED parameters only — never string-interpolate user input
    into SQL (injection-safe). Returns (column_names, rows).
    """
    body = {
        "warehouse_id": ctx.warehouse_id,
        "statement": statement,
        "parameters": params,
        "wait_timeout": "30s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }
    r = requests.post(
        f"{ctx.host}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {ctx.persona_token}", "Content-Type": "application/json"},
        json=body, timeout=60,
    )
    r.raise_for_status()
    resp = r.json()

    # Inline wait usually returns a terminal state; poll briefly if still running.
    statement_id = resp.get("statement_id")
    state_str = (resp.get("status") or {}).get("state")
    deadline = time.time() + 60
    while state_str in ("PENDING", "RUNNING") and time.time() < deadline:
        time.sleep(1.5)
        pr = requests.get(
            f"{ctx.host}/api/2.0/sql/statements/{statement_id}",
            headers={"Authorization": f"Bearer {ctx.persona_token}"}, timeout=30,
        )
        pr.raise_for_status()
        resp = pr.json()
        state_str = (resp.get("status") or {}).get("state")

    if state_str != "SUCCEEDED":
        status = resp.get("status") or {}
        err = (status.get("error") or {}).get("message") or state_str or "unknown error"
        raise RuntimeError(err)

    schema = (resp.get("manifest") or {}).get("schema", {})
    cols = [c.get("name") for c in schema.get("columns", [])]
    rows = (resp.get("result") or {}).get("data_array") or []
    return cols, rows


def _p(name: str, value: Optional[str]) -> dict:
    """Build a named STRING parameter. A None value is sent as SQL NULL so the
    UC function's `(p IS NULL OR col = p)` filters degrade to 'no filter'."""
    param: dict = {"name": name, "type": "STRING"}
    if value is not None and str(value).strip() != "":
        param["value"] = str(value)
    return param


def _rows_for_llm(cols: list[str], rows: list[list[Any]], limit: int = 20) -> str:
    """Compact JSON of a result set for Claude to reason over."""
    return json.dumps({
        "status": "OK",
        "columns": cols,
        "rows": rows[:limit],
        "row_count": len(rows),
    }, default=str)


def _genie_result_for_llm(result: GenieResult) -> str:
    if result.status != "COMPLETED":
        return json.dumps({"status": result.status, "error": result.error})
    payload: dict = {"status": "COMPLETED", "answer": result.answer_text}
    for s in result.steps:
        if s.kind == "query":
            payload["sql"] = s.sql
            payload["columns"] = s.columns
            payload["rows"] = s.rows[:20]
    return json.dumps(payload, default=str)


def _genie_step_to_dict(s) -> dict:
    return {"step_type": "genie", "kind": s.kind, "description": s.description,
            "sql": s.sql, "thoughts": s.thoughts, "answer": s.answer,
            "columns": s.columns, "rows": s.rows[:20]}


def _tool_step_dict(title: str, tool: str, cols: list[str], rows: list[list[Any]]) -> dict:
    return {"step_type": "tool", "title": title, "tool": tool,
            "columns": cols, "rows": rows[:20]}


# ---------------------------------------------------------------------------
# Executors
# ---------------------------------------------------------------------------
def _exec_query_genie(ctx: ToolContext, args: dict) -> ToolResult:
    """Open-ended NL analytics over ${catalog}.gold, via the Genie Conversation
    API, run as the persona SP. Streams Genie's own reasoning + SQL + rows."""
    question = (args.get("question") or "").strip()
    result = ctx.genie.ask(question, conversation_id=ctx.genie_conversation_id)
    ctx.genie_conversation_id = result.conversation_id or ctx.genie_conversation_id

    events: list[Event] = []
    steps: list[dict] = []
    genie_sql = None
    for gstep in result.steps:
        events.append(Event(type="genie_step", genie_step=gstep))
        steps.append(_genie_step_to_dict(gstep))
        if gstep.sql:
            genie_sql = gstep.sql

    meta = {
        "routed_to_genie": True,
        "genie_status": result.status,
        "genie_figure_out_s": round(result.figure_out_seconds, 3),
        "genie_reply_s": round(result.reply_seconds, 3),
        "genie_error": result.error,
        "genie_sql": genie_sql,
        "genie_conversation_id": ctx.genie_conversation_id,
        "steps": steps,
    }
    return ToolResult(llm_content=_genie_result_for_llm(result), events=events, meta=meta)


def _exec_get_market_share(ctx: ToolContext, args: dict) -> ToolResult:
    """Exact market-share numbers via the certified UC function
    ${catalog}.gold.f_market_share(category, region, period)."""
    stmt = (f"SELECT * FROM {ctx.catalog}.gold.f_market_share("
            ":category, :region, :period)")
    cols, rows = _execute_sql(ctx, stmt, [
        _p("category", args.get("category")),
        _p("region", args.get("region")),
        _p("period", args.get("period")),
    ])
    step = _tool_step_dict("Market share", "get_market_share", cols, rows)
    return ToolResult(
        llm_content=_rows_for_llm(cols, rows),
        events=[Event(type="tool_step", data=step)],
        meta={"steps": [step]},
    )


def _exec_get_promo_lift(ctx: ToolContext, args: dict) -> ToolResult:
    """Promotion lift via the certified UC function
    ${catalog}.gold.f_promo_lift(product_id, promo_id)."""
    stmt = (f"SELECT * FROM {ctx.catalog}.gold.f_promo_lift("
            ":product_id, :promo_id)")
    cols, rows = _execute_sql(ctx, stmt, [
        _p("product_id", args.get("product_id")),
        _p("promo_id", args.get("promo_id")),
    ])
    step = _tool_step_dict("Promotion lift", "get_promo_lift", cols, rows)
    return ToolResult(
        llm_content=_rows_for_llm(cols, rows),
        events=[Event(type="tool_step", data=step)],
        meta={"steps": [step]},
    )


def _exec_get_top_movers(ctx: ToolContext, args: dict) -> ToolResult:
    """Customer C only. Sub-second read from the Lakebase SYNCED table
    `market_share_snapshot` (reverse-ETL of Delta ${catalog}.gold.market_share).
    Demonstrates synced-table serving: OLTP-latency reads of governed gold data,
    no warehouse spin-up. Runs as the app SP through state.py."""
    region = args.get("region")
    category = args.get("category")
    limit = int(args.get("limit") or 5)
    cols, rows = state.top_movers(region=region, category=category, limit=limit)
    step = _tool_step_dict("Top movers (Lakebase synced snapshot)", "get_top_movers", cols, rows)
    return ToolResult(
        llm_content=_rows_for_llm(cols, rows),
        events=[Event(type="tool_step", data=step)],
        meta={"steps": [step]},
    )


def _exec_save_to_watchlist(ctx: ToolContext, args: dict) -> ToolResult:
    """Customer C only. Writes to the Lakebase OLTP table
    `watchlist(user_id, product_id, note, created_at)` as the app SP (owner)."""
    product_id = (args.get("product_id") or "").strip()
    note = (args.get("note") or "").strip()
    if not product_id:
        return ToolResult(llm_content=json.dumps({"status": "ERROR",
                          "error": "product_id is required to save to the watchlist."}))
    state.add_to_watchlist(user_id=ctx.username, product_id=product_id, note=note)
    confirmation = {"status": "SAVED", "user_id": ctx.username,
                    "product_id": product_id, "note": note or None}
    step = _tool_step_dict("Saved to watchlist", "save_to_watchlist",
                           ["user_id", "product_id", "note"],
                           [[ctx.username, product_id, note or None]])
    return ToolResult(
        llm_content=json.dumps(confirmation, default=str),
        events=[Event(type="tool_step", data=step)],
        meta={"steps": [step]},
    )


# ---------------------------------------------------------------------------
# The registry. Names match CONTRACT.md and the config/customer_*.yaml lists.
# ---------------------------------------------------------------------------
_REGISTRY: dict[str, ToolSpec] = {
    "query_genie": ToolSpec(
        schema=_fn_schema(
            "query_genie",
            ("Ask the company's governed data an open-ended natural-language question. "
             "Use for trends, comparisons, rankings, or anything not covered by a "
             "specific metric tool. Returns Genie's SQL, reasoning, and result rows."),
            {"question": {"type": "string",
                          "description": "A clear, self-contained data question in plain English."}},
            ["question"],
        ),
        executor=_exec_query_genie,
        notice="⏳ Querying your governed data with Genie — this can take a little longer…",
        summary="query_genie — open-ended natural-language analytics over the governed gold data.",
    ),
    "get_market_share": ToolSpec(
        schema=_fn_schema(
            "get_market_share",
            ("Return exact brand market-share rows from the certified market_share mart. "
             "Prefer this over query_genie whenever the user asks specifically about "
             "market share. All arguments are optional filters; omit one to leave it unfiltered."),
            {
                "category": {"type": "string", "description": "Product category filter, e.g. 'Bread'. Optional."},
                "region": {"type": "string", "description": "Region filter, e.g. 'Northeast'. Optional."},
                "period": {"type": "string", "description": "Reporting period, e.g. '2026-Q2'. Optional."},
            },
            [],
        ),
        executor=_exec_get_market_share,
        notice="📊 Looking up market share…",
        summary="get_market_share(category, region, period) — certified market-share numbers.",
    ),
    "get_promo_lift": ToolSpec(
        schema=_fn_schema(
            "get_promo_lift",
            ("Return promotion lift (baseline vs promo units and lift %) from the certified "
             "promo_performance mart. Use when the user asks how well a promotion performed."),
            {
                "product_id": {"type": "string", "description": "Product id filter. Optional."},
                "promo_id": {"type": "string", "description": "Promotion id filter. Optional."},
            },
            [],
        ),
        executor=_exec_get_promo_lift,
        notice="📈 Measuring promotion lift…",
        summary="get_promo_lift(product_id, promo_id) — certified promotion lift.",
    ),
    "get_top_movers": ToolSpec(
        schema=_fn_schema(
            "get_top_movers",
            ("Return the top brands by market share right now, read with sub-second latency "
             "from the Lakebase synced snapshot of the market_share mart. Use for a quick "
             "'who's on top' / leaderboard style question."),
            {
                "region": {"type": "string", "description": "Region filter. Optional."},
                "category": {"type": "string", "description": "Product category filter. Optional."},
                "limit": {"type": "integer", "description": "How many top rows to return (default 5)."},
            },
            [],
        ),
        executor=_exec_get_top_movers,
        notice="⚡ Fetching top movers from the Lakebase snapshot…",
        summary="get_top_movers(region, category, limit) — sub-second leaderboard from the Lakebase synced snapshot.",
        lakebase=True,
    ),
    "save_to_watchlist": ToolSpec(
        schema=_fn_schema(
            "save_to_watchlist",
            ("Save a product to the current user's watchlist so they can track it later. "
             "Confirm what was saved back to the user."),
            {
                "product_id": {"type": "string", "description": "The product id to watch."},
                "note": {"type": "string", "description": "Optional free-text note about why."},
            },
            ["product_id"],
        ),
        executor=_exec_save_to_watchlist,
        notice="⭐ Saving to your watchlist…",
        summary="save_to_watchlist(product_id, note) — write a product to the user's Lakebase watchlist.",
        lakebase=True,
    ),
}


def build_tools(enabled: list[str]) -> tuple[list[dict], dict[str, Executor]]:
    """Return (tool_schemas, executor_map) for ONLY the enabled tools.

    Unknown names are ignored (a config typo shouldn't crash the app). Order
    follows the config list so query_genie stays first.
    """
    schemas: list[dict] = []
    executors: dict[str, Executor] = {}
    for name in enabled:
        spec = _REGISTRY.get(name)
        if not spec:
            continue
        schemas.append(spec.schema)
        executors[name] = spec.executor
    return schemas, executors


def notice_for(tool_name: str) -> str:
    spec = _REGISTRY.get(tool_name)
    return spec.notice if spec else "⏳ Working on it…"


def summaries_for(enabled: list[str]) -> list[str]:
    """Human one-liners for the enabled tools, used to build the system prompt."""
    return [_REGISTRY[n].summary for n in enabled if n in _REGISTRY]
