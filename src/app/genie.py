"""Genie Conversation API client.

Runs entirely as the calling persona's service principal (token passed in), so
Unity Catalog enforces per-persona data access. We surface Genie's step-by-step
work by parsing message `attachments`: `query` blocks (the SQL Genie generated,
plus its description) and `text` blocks (the natural-language answer). This is the
verified reasoning surface returned by the raw Conversation API (not the trimmed
MCP wrapper).

Call sequence:
  1. start-conversation (first turn) OR conversations/{id}/messages (follow-up)
  2. poll get-message until status is terminal (COMPLETED / FAILED / ...)
  3. for any query attachment, fetch its result rows
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import requests


# Genie tags each reasoning chunk with a thought_type; map to friendly labels.
_THOUGHT_LABELS = {
    "THOUGHT_TYPE_DESCRIPTION": "What you asked",
    "THOUGHT_TYPE_UNDERSTANDING": "How I interpreted it",
    "THOUGHT_TYPE_DATA_SOURCING": "Data I used",
    "THOUGHT_TYPE_STEPS": "My plan",
    "THOUGHT_TYPE_ASSUMPTIONS": "Assumptions",
}


def clean_thoughts(thoughts: Any) -> list[dict]:
    """Normalize Genie's raw `thoughts` into [{label, content}] for clean display.

    Genie returns a list of {thought_type, content} objects (sometimes a JSON
    string). We map the type codes to human labels and tidy the content so the UI
    reads like the Genie product, not a raw Python repr.
    """
    if not thoughts:
        return []
    if isinstance(thoughts, str):
        try:
            thoughts = json.loads(thoughts)
        except (ValueError, TypeError):
            return [{"label": "Genie's reasoning", "content": thoughts}]
    if isinstance(thoughts, dict):
        thoughts = [thoughts]
    out: list[dict] = []
    for t in thoughts:
        if not isinstance(t, dict):
            continue
        label = _THOUGHT_LABELS.get(t.get("thought_type", ""), "Reasoning")
        content = (t.get("content") or "").replace("\\n", "\n").strip()
        # bullet-style plans read better as real markdown bullets
        content = content.replace("\n- ", "\n\n- ")
        if content:
            out.append({"label": label, "content": content})
    return out


@dataclass
class GenieStep:
    """One observable step of Genie's work, for display + logging."""
    kind: str                       # "text" | "query"
    description: Optional[str] = None
    sql: Optional[str] = None
    thoughts: Any = None            # structured [{thought_type, content}] list
    answer: Optional[str] = None
    columns: list[str] = field(default_factory=list)
    rows: list[list[Any]] = field(default_factory=list)

    @property
    def clean_thoughts(self) -> list[dict]:
        return clean_thoughts(self.thoughts)


@dataclass
class GenieResult:
    status: str
    conversation_id: Optional[str]
    message_id: Optional[str]
    steps: list[GenieStep] = field(default_factory=list)
    error: Optional[str] = None
    # timing split the demo cares about:
    figure_out_seconds: float = 0.0   # start -> COMPLETED (planning + SQL exec)
    reply_seconds: float = 0.0        # fetching result rows / assembling answer

    @property
    def answer_text(self) -> str:
        parts = [s.answer for s in self.steps if s.kind == "text" and s.answer]
        if parts:
            return "\n\n".join(parts)
        if self.error:
            return f"(Genie could not answer: {self.error})"
        return "(Genie returned no text answer.)"


class GenieClient:
    def __init__(self, host: str, token: str, space_id: str, poll_interval: float = 2.0,
                 timeout_seconds: float = 180.0):
        self.host = host.rstrip("/")
        self.space_id = space_id
        self.poll_interval = poll_interval
        self.timeout_seconds = timeout_seconds
        self._s = requests.Session()
        self._s.headers.update({"Authorization": f"Bearer {token}"})

    # --- low-level helpers ---
    def _url(self, path: str) -> str:
        return f"{self.host}/api/2.0/genie/spaces/{self.space_id}{path}"

    def _get(self, path: str) -> dict:
        r = self._s.get(self._url(path), timeout=60)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, body: dict) -> dict:
        r = self._s.post(self._url(path), json=body, timeout=60)
        r.raise_for_status()
        return r.json()

    # --- public API ---
    def ask(self, question: str, conversation_id: Optional[str] = None) -> GenieResult:
        """Ask a question; reuse conversation_id for follow-ups (Genie memory)."""
        t0 = time.time()
        if conversation_id:
            started = self._post(
                f"/conversations/{conversation_id}/messages",
                {"content": question},
            )
            message_id = started["message_id"] if "message_id" in started else started["id"]
        else:
            started = self._post("/start-conversation", {"content": question})
            conversation_id = started["conversation_id"]
            message_id = started["message_id"]

        msg = self._poll(conversation_id, message_id)
        figure_out = time.time() - t0

        status = msg.get("status", "UNKNOWN")
        if status != "COMPLETED":
            err = (msg.get("error") or {})
            return GenieResult(
                status=status,
                conversation_id=conversation_id,
                message_id=message_id,
                error=err.get("error") or err.get("message") or status,
                figure_out_seconds=figure_out,
            )

        t1 = time.time()
        steps = self._parse_attachments(conversation_id, message_id, msg.get("attachments", []))
        reply = time.time() - t1
        return GenieResult(
            status=status,
            conversation_id=conversation_id,
            message_id=message_id,
            steps=steps,
            figure_out_seconds=figure_out,
            reply_seconds=reply,
        )

    def _poll(self, conversation_id: str, message_id: str) -> dict:
        deadline = time.time() + self.timeout_seconds
        terminal = {"COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}
        while True:
            msg = self._get(f"/conversations/{conversation_id}/messages/{message_id}")
            if msg.get("status") in terminal:
                return msg
            if time.time() > deadline:
                msg["status"] = "TIMEOUT"
                return msg
            time.sleep(self.poll_interval)

    def _parse_attachments(self, conversation_id: str, message_id: str,
                           attachments: list[dict]) -> list[GenieStep]:
        steps: list[GenieStep] = []
        for att in attachments:
            att_id = att.get("attachment_id")
            if "text" in att:
                steps.append(GenieStep(kind="text", answer=att["text"].get("content")))
            elif "query" in att:
                q = att["query"]
                step = GenieStep(
                    kind="query",
                    description=q.get("description"),
                    sql=q.get("query"),
                    thoughts=q.get("thoughts"),
                )
                if att_id:
                    cols, rows = self._fetch_query_result(conversation_id, message_id, att_id)
                    step.columns, step.rows = cols, rows
                steps.append(step)
            # "viz" and "suggested_questions" attachments are ignored for the demo
        return steps

    def _fetch_query_result(self, conversation_id: str, message_id: str,
                            attachment_id: str) -> tuple[list[str], list[list[Any]]]:
        try:
            res = self._get(
                f"/conversations/{conversation_id}/messages/{message_id}"
                f"/attachments/{attachment_id}/query-result"
            )
        except requests.HTTPError:
            return [], []
        sr = (res.get("statement_response") or {})
        schema = (sr.get("manifest") or {}).get("schema", {})
        cols = [c.get("name") for c in schema.get("columns", [])]
        data = (sr.get("result") or {}).get("data_array") or []
        return cols, data[:50]
