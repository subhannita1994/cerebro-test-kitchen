# Module 4 (optional / stretch) — Fleet & Gateway ops

**Goal:** tie the day to the real 1000-customer story: govern the agent's model
calls at the **Unity AI Gateway**, and use **Lakebase branching** for safe
per-customer operations. **~45 min, pick one or both.**

**Teaches:** why this architecture scales — one governed model entry point, and
copy-on-write branches cheap enough to run per-customer across the fleet.

---

## Track A — Unity AI Gateway deep-dive

The orchestrator makes several model calls per question (decide → think →
synthesize) plus tool calls. The AI Gateway is the single control plane over all
of them. Using the Customer A/B/C model service(s):

- **Cost per turn/customer** — query `system.billing.usage` + the model service's
  inference table; attribute cost by `requester` (persona SP). "Each turn's N
  calls cost X; here's per-customer trend." *(GA)*
- **Observability** — the inference table logs every call's request/response,
  `latency_ms`, `time_to_first_byte_ms`, `requester`. Show decision vs. final
  calls. Reuse `src/app/observability_queries.sql`. *(GA)*
- **Routing / fallback** — add a fallback destination (Sonnet → a backup model);
  rate-limit the primary and show failover keeping the agent responsive. *(GA — verify in your workspace)*
- **Rate limits** — set a per-endpoint QPM limit; burst it; show throttling. *(GA)*
- **Guardrails** — attach a service policy (PII/safety); send a prompt with fake
  PII → masked/blocked before the model. *(Beta — flag it)*
- **Usage dashboard** — build an AI/BI dashboard over the inference table + system
  tables: calls, tokens, cost, latency, per-customer, guardrail hits.

**Punchline:** one governed entry point for a chatty agent, per-customer cost
attribution via the persona SP — the 1000-workspace margin story.

## Track B — Lakebase branching (fleet-safe ops)

Copy-on-write branches are instant and only changed pages cost storage — cheap
enough to run per-customer across 1000 workspaces. Using Customer C's Lakebase:

- **Safe schema migration** — branch the live chat DB, apply a migration
  (`ALTER TABLE messages ADD COLUMN ...`) on the branch, validate, then replay to
  prod. Show prod untouched during the test. *(pattern: run DDL as the owning app
  SP — see the repo's Lakebase migration notebook.)*
- **Point-in-time forensics** — branch as-of a timestamp `T` to reconstruct a
  conversation's exact `messages` + `agent_state`, compare to live.
- **Memory experiment** — branch, run an alternate agent-memory/summarization
  strategy over real threads, compare to prod, promote only if better.

**Punchline:** branch → test → promote is one CLI/API call; wrap it in a Job and
it runs identically across the whole fleet with per-customer validation gates.

---

## ✅ Module 4 done when
You can show, for at least one track: (A) a per-customer cost/latency view from
the Gateway inference table, or (B) a Lakebase branch where a change was tested
in isolation with production untouched.

> Deeper staging notes for both tracks live in the source runbook
> `cerebro-demo-runbook-lakebase-gateway.md` (organizer reference).
