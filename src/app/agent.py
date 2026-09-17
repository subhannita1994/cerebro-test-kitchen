"""The agent loop: Claude (via the Unity AI Gateway) decides which — if any —
of its CONFIGURED tools to call, then writes the answer.

This is the config-driven version. The set of tools handed to Claude is loaded
at runtime from the customer's `tools_config` YAML (query_genie always on;
get_market_share / get_promo_lift / get_top_movers / save_to_watchlist per
config), so the SAME code deploys to catalogs A / B / C with different behavior.

Loop:
  1. Send the conversation + the enabled tool schemas to Claude (extended
     thinking, streamed).
  2. If Claude answers directly -> return it.
  3. If Claude calls one or more tools -> dispatch to each executor, stream the
     tool's UI steps, feed the results back, and let Claude continue. Repeat
     until Claude stops calling tools (bounded).

We yield Event objects so the UI can stream Claude's thinking, a per-tool
"looking things up…" notice, Genie's step-by-step work, and UC/Lakebase tool
result tables. Component timing is captured on the TurnLog.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Iterator, Optional

import requests

import customer_config as cfg
import tracing
from genie import GenieClient
from tools import (
    Event, ToolContext, build_tools, notice_for, summaries_for,
)

# Structured stdout logging so every step is visible LIVE in the Databricks App
# Logs tab (…databricksapps.com/logz) between the prompt and the final answer.
logging.basicConfig(level=logging.INFO, format="%(asctime)s CEREBRO %(message)s")
log = logging.getLogger("cerebro.agent")

# --- Unity AI Gateway framing ----------------------------------------------
# Claude is reached through the Unity AI Gateway (the UC-native GA gateway, NOT a
# per-endpoint legacy serving config and NOT an external-model proxy). We POST
# the gateway URL with our own UC model-service FQN (${var.claude_model}); UC
# applies governance (usage + payload logging, guardrails, rate limits) and
# preserves caller identity, so each persona SP's own permissions apply.
GATEWAY_PATH = "/ai-gateway/mlflow/v1/chat/completions"

# --- config from the runtime resolver (customer_config) ----------------------
# NOTE: the hardcoded CLAUDE_MODEL / GENIE_SPACE_ID constants are GONE, and so is
# any reliance on ${var.*} in app.yaml — the whole point of the workshop is that
# behavior is CONFIG, resolved at runtime from the app's own name. WAREHOUSE_ID
# is the one value that stays in env (injected via app.yaml valueFrom).
WAREHOUSE_ID = os.environ["WAREHOUSE_ID"]

# Extended thinking: Claude streams a reasoning summary BEFORE its answer/tool
# call, so the user sees "why" while waiting. budget_tokens must be < max_tokens.
THINKING_BUDGET = 10240
MAX_TOKENS = 14336

# Safety valve for the generic tool loop (Claude could chain tools).
MAX_TOOL_ROUNDS = 5


def build_system_prompt(enabled: list[str]) -> str:
    """Describe the market-analytics assistant, listing whichever tools are
    enabled for THIS customer (built from the enabled set, not hardcoded)."""
    tool_lines = "\n".join(f"- {s}" for s in summaries_for(enabled))
    return (
        "You are Cerebro, a market-analytics assistant for one of FGF's retail "
        "customers. You help business users understand sales, market share, and "
        "promotion performance over the company's governed data.\n\n"
        "You have these tools available (only these — do not offer capabilities "
        "you don't have):\n"
        f"{tool_lines}\n\n"
        "How to decide:\n"
        "- Answer directly WITHOUT any tool for greetings, definitions, or "
        "clarifying questions that don't need the company's data.\n"
        "- Prefer a specific metric tool (e.g. get_market_share, get_promo_lift) "
        "when the question maps cleanly to it — it returns certified numbers.\n"
        "- Use query_genie for open-ended or exploratory questions.\n"
        "- Before calling a tool, briefly tell the user what you're about to look up.\n"
        "If a tool reports a permissions error, explain plainly that this account "
        "doesn't have access to that data and suggest what they can ask instead. "
        "Keep answers concise and business-friendly, and cite the numbers the "
        "tools return rather than inventing them."
    )


class Agent:
    def __init__(self, host: str, token: str, persona_key: str = "?", username: str = "?"):
        self.host = host.rstrip("/")
        self.token = token
        self.persona_key = persona_key
        self.username = username
        # everything below is resolved at runtime from the bundled customer config
        self.model = cfg.claude_model()
        self.catalog = cfg.catalog()
        self.lakebase_enabled = cfg.enable_lakebase_serving()
        self.enabled = cfg.enabled_tools()
        self.system_prompt = build_system_prompt(self.enabled)
        self.tool_schemas, self.executors = build_tools(self.enabled)
        self.genie = GenieClient(host=host, token=token, space_id=cfg.genie_space_id())
        log.info("[persona=%s] customer=%s catalog=%s tools=%s", persona_key,
                 cfg.customer(), self.catalog, ", ".join(self.enabled))

    # --- Claude streaming call ---------------------------------------------
    def _stream_claude(self, messages: list[dict]):
        """Call Claude with extended thinking, streaming.

        Yields ("reasoning", delta_text) as the thinking summary streams in, then
        ("message", assembled_message_dict). The assembled message keeps content
        as the block array (reasoning block WITH its signature + text block) plus
        tool_calls, so it can be appended verbatim to the tool loop — Anthropic
        requires the reasoning block be preserved unchanged when tool results are
        passed back.
        """
        body = {
            "model": self.model, "messages": messages,
            "max_tokens": MAX_TOKENS,
            "thinking": {"type": "enabled", "budget_tokens": THINKING_BUDGET},
            "stream": True,
        }
        if self.tool_schemas:
            body["tools"] = self.tool_schemas

        r = requests.post(
            f"{self.host}{GATEWAY_PATH}",
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
            json=body, timeout=180, stream=True,
        )
        r.raise_for_status()

        reasoning_parts: list[str] = []
        signature = ""
        text_parts: list[str] = []
        tool_calls: dict[int, dict] = {}
        finish_reason = None

        for line in r.iter_lines():
            if not line:
                continue
            s = line.decode("utf-8") if isinstance(line, bytes) else line
            if not s.startswith("data:"):
                continue
            payload = s[len("data:"):].strip()
            if payload == "[DONE]":
                break
            try:
                chunk = json.loads(payload)
            except ValueError:
                continue
            choice = (chunk.get("choices") or [{}])[0]
            delta = choice.get("delta") or {}
            if choice.get("finish_reason"):
                finish_reason = choice["finish_reason"]
            content = delta.get("content")
            if isinstance(content, list):
                for blk in content:
                    btype = blk.get("type")
                    if btype == "reasoning":
                        for sm in blk.get("summary", []):
                            t = sm.get("text", "")
                            if t:
                                reasoning_parts.append(t)
                                yield ("reasoning", t)
                            if sm.get("signature"):
                                signature = sm["signature"]
                    elif btype == "text":
                        t = blk.get("text", "")
                        if t:
                            text_parts.append(t)
            elif isinstance(content, str) and content:
                text_parts.append(content)
            for tc in delta.get("tool_calls", []) or []:
                idx = tc.get("index", 0)
                slot = tool_calls.setdefault(idx, {"id": None, "type": "function",
                                                   "function": {"name": "", "arguments": ""}})
                if tc.get("id"):
                    slot["id"] = tc["id"]
                fn = tc.get("function") or {}
                if fn.get("name"):
                    slot["function"]["name"] = fn["name"]
                if fn.get("arguments"):
                    slot["function"]["arguments"] += fn["arguments"]

        # reassemble the assistant message, preserving the reasoning block verbatim
        content_blocks: list[dict] = []
        reasoning_text = "".join(reasoning_parts)
        if reasoning_text:
            summary = {"type": "summary_text", "text": reasoning_text}
            if signature:
                summary["signature"] = signature
            content_blocks.append({"type": "reasoning", "summary": [summary]})
        answer_text = "".join(text_parts)
        if answer_text:
            content_blocks.append({"type": "text", "text": answer_text})

        message: dict = {"role": "assistant",
                         "content": content_blocks if content_blocks else answer_text}
        ordered_tcs = [tool_calls[i] for i in sorted(tool_calls)]
        if ordered_tcs:
            message["tool_calls"] = ordered_tcs
        message["_answer_text"] = answer_text
        message["_reasoning_text"] = reasoning_text
        message["_finish_reason"] = finish_reason
        yield ("message", message)

    # --- one turn -----------------------------------------------------------
    def run(self, history: list[dict], user_question: str,
            genie_conversation_id: Optional[str],
            turn_id: str, username: str, persona: str = "?") -> Iterator[Event]:
        """Drive one turn. `history` is prior [{role, content}] (no system).
        Yields Events; the final Event(type='answer') carries data with
        genie_conversation_id, collected steps, and a TurnLog with the component
        latency split for the analytics table."""
        messages = [{"role": "system", "content": self.system_prompt}, *history,
                    {"role": "user", "content": user_question}]
        collected_steps: list[dict] = []
        reasoning_all: list[str] = []
        tools_used: list[str] = []

        ctx = ToolContext(
            host=self.host, persona_token=self.token, persona_key=persona,
            username=username, catalog=self.catalog, warehouse_id=WAREHOUSE_ID,
            genie=self.genie, lakebase_enabled=self.lakebase_enabled,
            genie_conversation_id=genie_conversation_id,
        )

        tlog = tracing.TurnLog(turn_id=turn_id, username=username, persona=persona,
                               thread_id="", prompt=user_question).start()
        log.info("[persona=%s] PROMPT: %s", persona, user_question)

        def _drive(msgs):
            """Consume the streaming Claude call: forward reasoning deltas as
            Events, return the assembled message."""
            assembled = None
            for kind, val in self._stream_claude(msgs):
                if kind == "reasoning":
                    reasoning_all.append(val)
                    yield Event(type="reasoning", text=val)
                elif kind == "message":
                    assembled = val
            yield Event(type="_msg", data={"message": assembled})

        # --- generic tool loop ---
        final_msg = None
        for round_no in range(MAX_TOOL_ROUNDS + 1):
            t0 = time.time()
            msg = None
            for ev in _drive(messages):
                if ev.type == "_msg":
                    msg = ev.data["message"]
                else:
                    yield ev
            decision_s = round(time.time() - t0, 3)

            tool_calls = (msg or {}).get("tool_calls") or []
            if not tool_calls:
                # first round with no tools => decision time; later rounds roll
                # into the final-answer generation time (accumulate, don't clobber)
                if round_no == 0:
                    tlog.claude_decision_s = decision_s
                else:
                    tlog.claude_final_s += decision_s
                final_msg = msg
                break

            # timing: first round is the "decide" step, rest roll into final
            if round_no == 0:
                tlog.claude_decision_s = decision_s
            else:
                tlog.claude_final_s += decision_s

            log.info("[persona=%s] round %d -> %d tool call(s): %s", persona, round_no,
                     len(tool_calls), ", ".join(tc["function"]["name"] for tc in tool_calls))

            # append the assistant message VERBATIM (reasoning block + signature
            # preserved, as Anthropic requires for the tool loop).
            messages.append({k: v for k, v in msg.items() if not k.startswith("_")})

            for tc in tool_calls:
                name = tc["function"]["name"]
                try:
                    args = json.loads(tc["function"].get("arguments") or "{}")
                except ValueError:
                    args = {}
                tools_used.append(name)
                yield Event(type="tool_notice", text=notice_for(name))
                log.info("[persona=%s] TOOL %s(%s)", persona, name, json.dumps(args, default=str)[:200])

                executor = self.executors.get(name)
                if executor is None:
                    tool_content = json.dumps({"status": "ERROR",
                                               "error": f"Tool '{name}' is not available."})
                    messages.append({"role": "tool", "tool_call_id": tc["id"], "content": tool_content})
                    yield Event(type="error", text=f"Model requested an unavailable tool: {name}")
                    continue

                try:
                    result = executor(ctx, args)
                except requests.HTTPError as e:
                    code = e.response.status_code if e.response is not None else "?"
                    msg_txt = f"Tool {name} failed (HTTP {code})."
                    if code in (401, 403):
                        msg_txt = ("This account doesn't have access to that data "
                                   f"(HTTP {code} from {name}).")
                    tool_content = json.dumps({"status": "ERROR", "error": msg_txt})
                    messages.append({"role": "tool", "tool_call_id": tc["id"], "content": tool_content})
                    log.info("[persona=%s] TOOL %s HTTP error %s", persona, name, code)
                    continue
                except Exception as e:  # never crash a live turn on one tool
                    tool_content = json.dumps({"status": "ERROR", "error": str(e)[:300]})
                    messages.append({"role": "tool", "tool_call_id": tc["id"], "content": tool_content})
                    log.info("[persona=%s] TOOL %s error: %s", persona, name, str(e)[:200])
                    continue

                # merge tool metadata into the log + collected steps
                meta = result.meta or {}
                if meta.get("routed_to_genie"):
                    tlog.routed_to_genie = True
                    tlog.genie_status = meta.get("genie_status")
                    tlog.genie_figure_out_s = meta.get("genie_figure_out_s", 0.0)
                    tlog.genie_reply_s = meta.get("genie_reply_s", 0.0)
                    tlog.genie_error = meta.get("genie_error")
                    if meta.get("genie_sql"):
                        tlog.genie_sql = meta["genie_sql"]
                    ctx.genie_conversation_id = meta.get("genie_conversation_id", ctx.genie_conversation_id)
                for s in meta.get("steps", []):
                    collected_steps.append(s)

                for ui_ev in result.events:
                    yield ui_ev

                messages.append({"role": "tool", "tool_call_id": tc["id"],
                                 "content": result.llm_content})

            if round_no == MAX_TOOL_ROUNDS:
                log.info("[persona=%s] hit MAX_TOOL_ROUNDS; asking Claude to wrap up", persona)

        # --- final answer -------------------------------------------------
        answer = (final_msg or {}).get("_answer_text") or "(no answer)"
        tlog.answer = answer
        tlog.claude_reasoning = "\n".join(reasoning_all) or None
        tlog.tools_used = ",".join(dict.fromkeys(tools_used)) or None  # de-dup, keep order
        log.info("[persona=%s] DONE (tools_used=%s)", persona, tlog.tools_used or "none")

        yield Event(type="answer", text=answer,
                    data={"genie_conversation_id": ctx.genie_conversation_id,
                          "steps": collected_steps, "turn_log": tlog})
