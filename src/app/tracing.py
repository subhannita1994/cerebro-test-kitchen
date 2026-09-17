"""Observability: persist one analytics row per user turn to a UC Delta table.

Design (per Databricks best practice for a thin agent app):
  - Lakebase  -> live operational state (chat history/resume)  [state.py]
  - UC Delta  -> analytics/monitoring                          [this module]
  - AI Gateway inference table -> raw LLM payloads + cost (governed in UC)

The `/logz` stdout stream (see agent.py) is for live debugging only and is
ephemeral. This module is the DURABLE analytics layer: it writes a structured
row — who asked, which tools ran, whether it routed to Genie, and the component
latency split (Claude decide / Genie figure-out / Genie reply / Claude final) —
that the AI/BI monitoring dashboard reads.

Writes run as the APP's own service principal via the SQL Statement Execution
API, so the log is app-owned regardless of which persona made the request. The
target table lives in the customer's own catalog: ${CATALOG}.gold.agent_turn_log
(see CONTRACT.md ## Additions). It is created on first use (idempotent).
"""
from __future__ import annotations

import os
import time
import logging
from dataclasses import dataclass, field
from typing import Optional

import requests

import customer_config as cfg

log = logging.getLogger("cerebro.tracing")

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
WAREHOUSE_ID = os.environ["WAREHOUSE_ID"]                 # app.yaml valueFrom sql-warehouse
CATALOG = cfg.catalog()                                  # from the bundled customer config
# App-owned analytics table in the customer's catalog. Override with an explicit
# CEREBRO_LOG_TABLE if a workshop wants it elsewhere.
LOG_TABLE = os.environ.get("CEREBRO_LOG_TABLE", f"{CATALOG}.gold.agent_turn_log")

_table_ready = False


@dataclass
class TurnLog:
    """One user turn's analytics record (mirrors the agent_turn_log table)."""
    turn_id: str
    username: str
    persona: str
    thread_id: str
    prompt: str
    routed_to_genie: bool = False
    tools_used: Optional[str] = None
    claude_decision_s: float = 0.0
    genie_figure_out_s: float = 0.0
    genie_reply_s: float = 0.0
    claude_final_s: float = 0.0
    total_s: float = 0.0
    genie_status: Optional[str] = None
    genie_error: Optional[str] = None
    genie_sql: Optional[str] = None
    answer: Optional[str] = None
    claude_reasoning: Optional[str] = None
    _t0: float = field(default=0.0, repr=False)

    def start(self) -> "TurnLog":
        self._t0 = time.time()
        return self

    def finish(self, app_token: str) -> None:
        """Stamp total time and persist the row (best-effort; never breaks a turn)."""
        self.total_s = round(time.time() - self._t0, 3)
        try:
            _ensure_table(app_token)
            _insert(self, app_token)
        except Exception as e:  # analytics must never break the chat
            log.info("turn-log write failed (non-fatal): %s", str(e)[:200])


def _exec(stmt: str, app_token: str) -> None:
    r = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {app_token}", "Content-Type": "application/json"},
        json={"warehouse_id": WAREHOUSE_ID, "statement": stmt, "wait_timeout": "30s"},
        timeout=40,
    )
    r.raise_for_status()


def _ensure_table(app_token: str) -> None:
    """Create the analytics table once per process (idempotent). Keeps the app
    self-contained so the workshop's AI swimlane doesn't depend on the data
    swimlane having created it first."""
    global _table_ready
    if _table_ready:
        return
    _exec(f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
            turn_id STRING, event_time TIMESTAMP, username STRING, persona STRING,
            thread_id STRING, prompt STRING, routed_to_genie BOOLEAN, tools_used STRING,
            claude_decision_s DOUBLE, genie_figure_out_s DOUBLE, genie_reply_s DOUBLE,
            claude_final_s DOUBLE, total_s DOUBLE, genie_status STRING,
            genie_error STRING, genie_sql STRING, answer STRING, claude_reasoning STRING
        ) USING DELTA
    """, app_token)
    _table_ready = True


def _sql_lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _insert(t: TurnLog, app_token: str) -> None:
    cols = ["turn_id", "event_time", "username", "persona", "thread_id", "prompt",
            "routed_to_genie", "tools_used", "claude_decision_s", "genie_figure_out_s",
            "genie_reply_s", "claude_final_s", "total_s", "genie_status",
            "genie_error", "genie_sql", "answer", "claude_reasoning"]
    vals = [
        _sql_lit(t.turn_id), "current_timestamp()", _sql_lit(t.username),
        _sql_lit(t.persona), _sql_lit(t.thread_id), _sql_lit(t.prompt[:4000]),
        _sql_lit(t.routed_to_genie), _sql_lit(t.tools_used),
        _sql_lit(t.claude_decision_s), _sql_lit(t.genie_figure_out_s),
        _sql_lit(t.genie_reply_s), _sql_lit(t.claude_final_s), _sql_lit(t.total_s),
        _sql_lit(t.genie_status),
        _sql_lit((t.genie_error or "")[:1000] or None),
        _sql_lit((t.genie_sql or "")[:4000] or None),
        _sql_lit((t.answer or "")[:4000] or None),
        _sql_lit((t.claude_reasoning or "")[:4000] or None),
    ]
    _exec(f"INSERT INTO {LOG_TABLE} ({', '.join(cols)}) VALUES ({', '.join(vals)})", app_token)
