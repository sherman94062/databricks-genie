# Genie vs. Python Agents: When to Use Which

**Audience:** Engineering leadership · **Length:** 1 page · **TL;DR on line 1**

**Recommendation:** Keep Genie for human-facing NL-to-SQL; use purpose-built Python agents for programmatic and high-volume workloads. Don't treat it as either/or — both hit the same warehouses and the same Unity Catalog, so the split is about *who's asking*, not *where the data lives*.

---

## What you're actually paying for with Genie

A Genie question has two distinct cost lines in `system.billing.usage`:

| Component | SKU family | Goes away if we self-host? |
|---|---|---|
| NL→SQL generation (the LLM call) | `*GENIE*` / AI-assist SKUs | ✅ Yes — replaced by Anthropic/OpenAI bill |
| SQL execution on warehouse | `*SERVERLESS_SQL*` | ❌ No — same cost either way |

Only the first line is *replaceable*. On a typical question, LLM cost is in the low single-digit cents; warehouse compute dominates. Moving NL→SQL into Python doesn't reduce spend meaningfully unless call volume is very high — but it *does* change other things.

## What Genie gives you that a Python agent doesn't (for free)

- **Unity Catalog grounding** — tables, columns, comments, tags, and PII masks automatically in context
- **Curated examples per space** — SMEs add canonical SQL; Genie learns the business's idioms
- **Governance perimeter** — queries never leave Databricks; audit log, lineage, row/column security enforced natively
- **Zero infra** — no prompt versioning, no LLM provider keys, no retries, no observability stack to build
- **Native UI** — Databricks chat, dashboards, alerts, and embedding all work out of the box

## What a Python agent gives you that Genie doesn't

- **Cost predictability** — pick the model, set token limits, cache aggressively
- **Custom logic** — query rewriting, multi-step reasoning, routing across sources (Databricks + APIs + warehouses)
- **Prompt engineering control** — version in git, A/B test, regression-test
- **Embed anywhere** — customer-facing apps, Slack bots, internal tools — no Databricks UI dependency

## The decision rule

Split by **audience and call pattern**, not by cost:

| Use case | Pick |
|---|---|
| Analysts doing ad-hoc exploration | **Genie** |
| Business users asking governed questions in the UI | **Genie** |
| Dashboards with NL follow-up | **Genie** |
| Scheduled pipelines doing NL→SQL | **Python agent** |
| Customer-facing / embedded experiences | **Python agent** |
| High-frequency identical questions | **Pre-canned SQL** (skip both LLMs) |

## Cost-optimization pattern regardless of which you pick

1. **Trusted-query router in front of both.** High-frequency questions ("spend last 30 days", "top SKUs") hit a pre-approved parameterized SQL library — no LLM involved, no warehouse re-planning. This is the biggest lever and applies in all three columns above.
2. **Cache results on stable time windows.** Daily finance questions don't need fresh execution every time.
3. **Right-size the warehouse.** Small serverless warehouses for Genie/agent traffic; scale only for heavy workloads. Warehouse cost dwarfs LLM cost on almost every question.

## What this project demonstrates

- `GenieClient` wrapper (retry, polling, rate-limit aware)
- Query router with trusted-query library (reduces both LLM and warehouse cost on frequent questions)
- Session log + per-statement cost attribution via `system.billing.usage` and the Query History API
- `cli genie-vs-warehouse` — prints the *replaceable* dollar figure so the decision is quantitative, not anecdotal
- Streamlit UI with live cost/activity tab

## Suggested Zepz rollout

1. **Stand up Genie spaces per domain** (Payments, Compliance, Growth). SMEs curate example queries.
2. **Wrap a shared Python client library** (this repo's pattern) that adds: (a) trusted-query router, (b) session logging with statement_id, (c) cost attribution dashboards for FinOps.
3. **Use Python agents where volume/cost-predictability matter** — scheduled risk scoring, customer-facing "ask your transaction history," etc.
4. **Monitor weekly** via the `genie-vs-warehouse` report. Move the biggest LLM-cost questions into the trusted-query library first; only self-host NL→SQL for workloads where Genie LLM spend crosses a defined threshold (e.g., >$N/month).

---
*Prepared by Mike Sherman · April 2026*
