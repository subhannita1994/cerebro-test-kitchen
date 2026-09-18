"""Cerebro Test Kitchen — Streamlit chat app on Databricks Apps.

Config-driven market-analytics assistant. Custom login (no Databricks identity)
-> persona -> service principal. Claude decides which of its CONFIGURED tools to
call (query_genie + per-customer metric/Lakebase tools); the UI streams Claude's
thinking, a per-tool "looking things up…" banner, Genie's own thoughts + SQL +
results, and UC/Lakebase tool result tables. Conversation history + resume live
in Lakebase; every turn is traced with a per-component latency breakdown.

The SAME image deploys to catalogs A/B/C — only the env/config changes what runs.
"""
from __future__ import annotations

import os
import uuid

import streamlit as st

import auth
import customer_config as cfg
import state
from agent import Agent

st.set_page_config(page_title="Cerebro Assistant", page_icon="🥐", layout="wide")

# The Apps runtime injects DATABRICKS_HOST as a bare hostname (no scheme); make
# sure it has https:// (feeds the agent -> gateway/genie/tool URLs).
_HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
DATABRICKS_HOST = _HOST if _HOST.startswith("http") else f"https://{_HOST}"
CATALOG = cfg.catalog()
ENABLE_LAKEBASE_SERVING = cfg.enable_lakebase_serving()


# --- one-time schema/seed ---------------------------------------------------
@st.cache_resource
def _bootstrap():
    state.init_schema()
    if ENABLE_LAKEBASE_SERVING:
        # Customer C: ensure the OLTP watchlist exists (owned by the app SP).
        try:
            state.init_watchlist()
        except Exception:
            pass
    return True


_bootstrap()


# --- session helpers --------------------------------------------------------
def _reset_login():
    for k in ("username", "persona", "thread_id"):
        st.session_state.pop(k, None)


class _DictStep:
    """Adapts a persisted Genie step dict back to the attribute API render_genie_step expects."""
    def __init__(self, d: dict):
        self.kind = d.get("kind")
        self.description = d.get("description")
        self.sql = d.get("sql")
        self.thoughts = d.get("thoughts")
        self.answer = d.get("answer")
        self.columns = d.get("columns") or []
        self.rows = d.get("rows") or []

    @property
    def clean_thoughts(self):
        from genie import clean_thoughts
        return clean_thoughts(self.thoughts)


def render_genie_step(gstep) -> None:
    """Render one Genie step (reasoning + SQL + result rows), Genie-style."""
    if gstep.kind != "query":
        return
    with st.expander("🧠 How Genie worked it out", expanded=False):
        for th in gstep.clean_thoughts:
            st.markdown(f"**{th['label']}**")
            st.markdown(th["content"])
        if gstep.sql:
            st.markdown("**Query Genie ran**")
            st.code(gstep.sql, language="sql")
        if gstep.rows:
            st.markdown("**Result**")
            st.dataframe(
                {c: [r[i] for r in gstep.rows] for i, c in enumerate(gstep.columns)},
                use_container_width=True, hide_index=True,
            )


def render_tool_step(step: dict) -> None:
    """Render one UC/Lakebase tool result (a titled table)."""
    title = step.get("title") or "Tool result"
    cols = step.get("columns") or []
    rows = step.get("rows") or []
    with st.expander(f"🔧 {title}", expanded=False):
        if rows and cols:
            st.dataframe(
                {c: [r[i] for r in rows] for i, c in enumerate(cols)},
                use_container_width=True, hide_index=True,
            )
        else:
            st.caption("No rows returned.")


def render_step(step: dict) -> None:
    """Dispatch a persisted step dict to the right renderer."""
    if step.get("step_type") == "tool" or step.get("title"):
        render_tool_step(step)
    else:
        render_genie_step(_DictStep(step))


# --- login screen -----------------------------------------------------------
if "persona" not in st.session_state:
    st.title("🥐 Cerebro Assistant")
    st.caption("Sign in to ask questions about your market, sales, and promotions data.")
    with st.form("login"):
        username = st.text_input("Username", placeholder="alex or morgan")
        password = st.text_input("Password", type="password", placeholder="demo")
        submitted = st.form_submit_button("Sign in")
    if submitted:
        persona = state.authenticate(username.strip(), password)
        if persona:
            st.session_state.username = username.strip()
            st.session_state.persona = persona
            st.rerun()
        else:
            st.error("Invalid credentials. Try alex/demo (analyst) or morgan/demo (manager).")
    st.info("Demo logins — **alex** (Analyst, limited access) · **morgan** (Manager, full access). "
            "Both are non-Databricks users mapped to different service principals.")
    st.stop()


username = st.session_state.username
persona_key = st.session_state.persona
persona = auth.get_persona(persona_key)

# --- sidebar: identity + thread list (resume) ------------------------------
with st.sidebar:
    st.markdown(f"**{username}**")
    st.caption(f"Persona: {persona.label}")
    if CATALOG:
        st.caption(f"Catalog: `{CATALOG}`")
    if st.button("Sign out"):
        _reset_login()
        st.rerun()
    st.divider()
    if st.button("➕ New conversation", use_container_width=True):
        st.session_state.pop("thread_id", None)
        st.rerun()
    st.markdown("##### Past conversations")
    for t in state.list_threads(username):
        if st.button(t.title, key=f"th_{t.thread_id}", use_container_width=True):
            st.session_state.thread_id = t.thread_id
            st.rerun()

# --- main: chat -------------------------------------------------------------
st.title("🥐 Cerebro Assistant")

thread_id = st.session_state.get("thread_id")

# replay history for a resumed/active thread
if thread_id:
    for m in state.get_messages(thread_id):
        with st.chat_message("user" if m["role"] == "user" else "assistant"):
            st.markdown(m["content"] or "")
            for s in (m.get("steps") or []):
                render_step(s)

prompt = st.chat_input("Ask about market share, sales, or promotions…")
if prompt:
    # create a thread lazily on first message
    if not thread_id:
        thread_id = state.create_thread(username, prompt)
        st.session_state.thread_id = thread_id

    with st.chat_message("user"):
        st.markdown(prompt)
    state.add_message(thread_id, "user", prompt)

    # build agent as the persona's service principal
    token = auth.get_token(persona_key)
    agent = Agent(host=DATABRICKS_HOST, token=token,
                  persona_key=persona_key, username=username)

    history = [{"role": m["role"], "content": m["content"]}
               for m in state.get_messages(thread_id)
               if m["role"] in ("user", "assistant") and m["content"]][:-1]
    genie_cid = state.last_genie_conversation_id(thread_id)

    turn_id = str(uuid.uuid4())
    with st.chat_message("assistant"):
        thinking_box = st.empty()      # live-updating Claude reasoning
        answer_box = st.empty()
        reasoning_buf = []
        final_answer, final_data = "", {}
        for ev in agent.run(history, prompt, genie_cid,
                            turn_id=turn_id, username=username, persona=persona_key):
            if ev.type == "reasoning":
                reasoning_buf.append(ev.text or "")
                with thinking_box.expander("🧠 Claude is thinking…", expanded=True):
                    st.markdown("".join(reasoning_buf))
            elif ev.type == "tool_notice":
                st.warning(ev.text)
            elif ev.type == "genie_step":
                render_genie_step(ev.genie_step)
            elif ev.type == "tool_step":
                render_tool_step(ev.data)
            elif ev.type == "answer":
                final_answer = ev.text or ""
                final_data = ev.data or {}
                if reasoning_buf:
                    with thinking_box.expander("🧠 Claude's reasoning", expanded=False):
                        st.markdown("".join(reasoning_buf))
                answer_box.markdown(final_answer)
            elif ev.type == "error":
                st.error(ev.text)

    state.add_message(
        thread_id, "assistant", final_answer,
        steps=final_data.get("steps"),
        genie_conversation_id=final_data.get("genie_conversation_id"),
    )

    # persist the analytics row (component latency split, tools used, cost keys)
    tlog = final_data.get("turn_log")
    if tlog is not None:
        tlog.thread_id = thread_id
        tlog.finish(auth.get_app_token())
