"""Conversation history + resumable agent state, stored in Lakebase (Postgres),
plus the Customer-C serving helpers (synced-snapshot read + OLTP watchlist).

Native Databricks managed Postgres. Chat-memory tables (all customers):
  users        - demo login (username/password) -> persona map (no Databricks identity)
  threads      - one row per conversation, for the "resume a past chat" list
  messages     - full turn history; carries genie_conversation_id so a resumed
                 thread continues Genie's own memory
  agent_state  - rolling summary + last genie conversation id per thread

Customer C only (gated on ENABLE_LAKEBASE_SERVING at the app layer):
  market_share_snapshot - a SYNCED table (reverse-ETL of Delta gold.market_share);
                          read-only here, refreshed by the synced-table pipeline.
  watchlist             - OLTP table the chat writes to (user_id, product_id, note).

Auth to Postgres: username = the app service principal's client id; password =
a short-lived Databricks OAuth token minted by the SDK for the database instance.
All connections here run AS THE APP SP (the table owner), matching the managed-
Lakebase ownership model in lakebase_migration_notebook.py.
"""
from __future__ import annotations

import os
import time
import uuid
import hashlib
import threading
import json
from dataclasses import dataclass
from typing import Any, Optional

import psycopg
from databricks.sdk import WorkspaceClient

import customer_config as cfg

# Lakebase connection details come from the bundled customer config (resolved at
# runtime), NOT from env — see customer_config.py for why app.yaml can't carry them.
DB_HOST = cfg.lakebase_host()                        # read_write_dns
DB_NAME = cfg.lakebase_database()
DB_INSTANCE = cfg.lakebase_instance()                # for credential minting
# Postgres role = the app's own service principal. The Apps runtime injects its
# client id as DATABRICKS_CLIENT_ID; allow an explicit override via LAKEBASE_USER.
DB_USER = os.environ.get("LAKEBASE_USER") or os.environ["DATABRICKS_CLIENT_ID"]
# The app SP can't CREATE in the locked-down `public` schema, so the app uses its
# OWN schema — named after the catalog, so the four apps sharing one Lakebase
# instance stay isolated (cerebro_dev / cerebro_a / ...). The app SP CREATEs it
# (via the database's CAN_CONNECT_AND_CREATE grant) and therefore OWNS it. Every
# connection sets search_path here, so unqualified table names resolve to it
# (falling back to `public` for the Customer-C synced snapshot).
APP_SCHEMA = os.environ.get("LAKEBASE_SCHEMA") or cfg.catalog()

_w = WorkspaceClient()
_cred_lock = threading.Lock()
_cred: tuple[str, float] = ("", 0.0)


def _password() -> str:
    """Mint (and cache) a Lakebase OAuth credential; used as the PG password."""
    global _cred
    with _cred_lock:
        if _cred[1] - 120 > time.time():
            return _cred[0]
        cred = _w.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=[DB_INSTANCE]
        )
        # token valid ~1h; refresh a couple minutes early
        _cred = (cred.token, time.time() + 3300)
        return _cred[0]


def _connect() -> psycopg.Connection:
    return psycopg.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=_password(),
        sslmode="require", autocommit=True,
        options=f"-c search_path={APP_SCHEMA},public",
    )


def _hash(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


# --- schema + seed --------------------------------------------------------
DEMO_USERS = [
    # username, password, persona
    ("alex",  "demo", "A"),   # analyst, less privileged
    ("morgan", "demo", "B"),  # manager, full access
]


def init_schema() -> None:
    """Create the chat-memory tables (all customers) and seed demo logins."""
    with _connect() as c, c.cursor() as cur:
        # Create the app-owned schema first (public is locked down). search_path
        # already points here, so the unqualified tables below land in it.
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{APP_SCHEMA}"')
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                persona TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS threads (
                thread_id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                title TEXT,
                created_at DOUBLE PRECISION,
                updated_at DOUBLE PRECISION
            );
            CREATE TABLE IF NOT EXISTS messages (
                message_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT,
                steps_json TEXT,
                created_at DOUBLE PRECISION,
                genie_conversation_id TEXT
            );
            CREATE TABLE IF NOT EXISTS agent_state (
                thread_id TEXT PRIMARY KEY,
                summary TEXT,
                last_genie_conversation_id TEXT,
                updated_at DOUBLE PRECISION
            );
        """)
        for username, pw, persona in DEMO_USERS:
            cur.execute(
                "INSERT INTO users (username, password_hash, persona) VALUES (%s,%s,%s) "
                "ON CONFLICT (username) DO UPDATE SET password_hash=EXCLUDED.password_hash, persona=EXCLUDED.persona",
                (username, _hash(pw), persona),
            )


def init_watchlist() -> None:
    """Customer C only. Create the OLTP watchlist table (owned by the app SP).
    Called from setup_lakebase.py and, defensively, at app start when serving
    is enabled. The synced table market_share_snapshot is NOT created here — it
    is provisioned by the synced-table pipeline (see src/lakebase/setup_lakebase.py)."""
    with _connect() as c, c.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                user_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                note TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)


# --- auth ------------------------------------------------------------------
def authenticate(username: str, password: str) -> Optional[str]:
    """Return the persona key ('A'/'B') if credentials match, else None."""
    with _connect() as c, c.cursor() as cur:
        cur.execute("SELECT password_hash, persona FROM users WHERE username=%s", (username,))
        row = cur.fetchone()
    if row and row[0] == _hash(password):
        return row[1]
    return None


# --- threads + messages ----------------------------------------------------
@dataclass
class Thread:
    thread_id: str
    title: str
    updated_at: float


def list_threads(username: str) -> list[Thread]:
    with _connect() as c, c.cursor() as cur:
        cur.execute(
            "SELECT thread_id, COALESCE(title,'(untitled)'), updated_at FROM threads "
            "WHERE username=%s ORDER BY updated_at DESC", (username,))
        return [Thread(*r) for r in cur.fetchall()]


def create_thread(username: str, title: str) -> str:
    tid = str(uuid.uuid4())
    now = time.time()
    with _connect() as c, c.cursor() as cur:
        cur.execute(
            "INSERT INTO threads (thread_id, username, title, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,%s)", (tid, username, title[:80], now, now))
    return tid


def add_message(thread_id: str, role: str, content: str,
                steps: Optional[list] = None, genie_conversation_id: Optional[str] = None) -> None:
    now = time.time()
    with _connect() as c, c.cursor() as cur:
        cur.execute(
            "INSERT INTO messages (message_id, thread_id, role, content, steps_json, created_at, genie_conversation_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (str(uuid.uuid4()), thread_id, role, content,
             json.dumps(steps) if steps else None, now, genie_conversation_id))
        cur.execute("UPDATE threads SET updated_at=%s WHERE thread_id=%s", (now, thread_id))


def get_messages(thread_id: str) -> list[dict]:
    with _connect() as c, c.cursor() as cur:
        cur.execute(
            "SELECT role, content, steps_json, genie_conversation_id FROM messages "
            "WHERE thread_id=%s ORDER BY created_at ASC", (thread_id,))
        out = []
        for role, content, steps_json, gcid in cur.fetchall():
            out.append({
                "role": role, "content": content,
                "steps": json.loads(steps_json) if steps_json else None,
                "genie_conversation_id": gcid,
            })
        return out


def last_genie_conversation_id(thread_id: str) -> Optional[str]:
    """Most recent genie conversation id in a thread, so a resumed thread keeps Genie memory."""
    with _connect() as c, c.cursor() as cur:
        cur.execute(
            "SELECT genie_conversation_id FROM messages WHERE thread_id=%s "
            "AND genie_conversation_id IS NOT NULL ORDER BY created_at DESC LIMIT 1", (thread_id,))
        row = cur.fetchone()
    return row[0] if row else None


# --- Customer C serving (Lakebase) -----------------------------------------
def top_movers(region: Optional[str] = None, category: Optional[str] = None,
               limit: int = 5) -> tuple[list[str], list[list[Any]]]:
    """Sub-second read from the SYNCED snapshot `market_share_snapshot` (reverse-
    ETL of Delta gold.market_share). Columns mirror gold.market_share. Optional
    region/category filters; ranked by share_pct desc. Returns (columns, rows)."""
    limit = max(1, min(int(limit or 5), 100))
    cols = ["period", "region", "category", "brand",
            "brand_revenue", "category_revenue", "share_pct"]
    where, params = [], []
    if region:
        where.append("region = %s")
        params.append(region)
    if category:
        where.append("category = %s")
        params.append(category)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    sql = (f"SELECT {', '.join(cols)} FROM market_share_snapshot "
           f"{clause} ORDER BY share_pct DESC NULLS LAST LIMIT %s")
    params.append(limit)
    with _connect() as c, c.cursor() as cur:
        cur.execute(sql, tuple(params))
        rows = [list(r) for r in cur.fetchall()]
    return cols, rows


def add_to_watchlist(user_id: str, product_id: str, note: str = "") -> None:
    """Write one row to the OLTP watchlist as the app SP (owner)."""
    with _connect() as c, c.cursor() as cur:
        cur.execute(
            "INSERT INTO watchlist (user_id, product_id, note) VALUES (%s,%s,%s)",
            (user_id, product_id, note or None))
