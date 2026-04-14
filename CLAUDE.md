# CLAUDE.md — Databricks Genie API Agent Project

## Project Context

This project explores and builds tooling around the **Databricks AI/BI Genie Conversation API**,
with a dual purpose:

1. **Personal learning** — deepen hands-on familiarity with Genie for use at a new Data Engineer
   role at Zepz (starting soon), working under Drew Stooksberry (VP Engineering, Data).
2. **Proof-of-concept agents** — demonstrate how purpose-built API agents can reduce Databricks
   costs vs. open-ended Genie UI sessions, a priority given Zepz's cost-conscious culture.

The developer (Mike Sherman) has 50+ years in technology, a strong Python background, deep
Databricks experience (former Imply/Apache Druid support engineer, built Target's analytics
platform), and has built multiple MCP servers and AI agent projects. Prefer concise, senior-level
explanations — no hand-holding on fundamentals.

---

## Environment

- **Databricks account**: Free Edition (personal, AWS)
- **Authentication**: Personal OAuth token (Databricks credential already configured)
- **Warehouse**: Serverless SQL warehouse (already accessible)
- **Python**: 3.11+
- **Package manager**: pip (use `--break-system-packages` if needed, or a venv)
- **IDE workflow**: Claude Code (primary development tool)

### Environment Variables (set in `.env`, never commit)

```
DATABRICKS_HOST=https://<your-workspace>.azuredatabricks.net  # or .cloud.databricks.com for AWS
DATABRICKS_TOKEN=<your-personal-access-token-or-oauth-token>
DATABRICKS_WAREHOUSE_ID=<your-sql-warehouse-id>
GENIE_SPACE_ID=<your-genie-space-id>   # set once first space is created
```

---

## Key Constraints & Known Limitations (Free Edition)

- **Rate limit**: Genie API free tier is limited to **5 questions per minute per workspace**
  (best-effort, Public Preview). Build retry logic with exponential backoff into all clients.
- **Row limit**: Genie API returns a maximum of **5,000 rows** per query result.
- **No account console**: Cannot create service principals on Free Edition. Use personal OAuth
  token (U2M) for all API calls during development.
- **No commercial use**: This workspace is for learning/prototyping only. Production work happens
  on Zepz's paid workspace.
- **Poll interval**: Poll for query status every 1–5 seconds; timeout after 10 minutes.

---

## Project Goals (Phased)

### Phase 1 — Foundation (Start Here)
- [ ] Set up project structure, `.env`, and Databricks SDK
- [ ] Create a Genie space in the UI pointed at sample/test data in Unity Catalog
- [ ] Verify Genie space works interactively in the UI
- [ ] Make a first successful Genie Conversation API call (start conversation, poll, retrieve result)
- [ ] Build a minimal Python client class wrapping the API

### Phase 2 — Agent Patterns
- [ ] Build a **query router**: classifies incoming questions as simple (use pre-canned SQL) vs.
  complex (send to Genie), minimizing compute spend
- [ ] Build a **Slack-style CLI bot**: accepts NL questions, calls Genie API, prints results
- [ ] Build a **parameterized trusted query library**: registers common questions as Genie
  example SQL to avoid LLM SQL generation cost on high-frequency queries
- [ ] Implement a **cost monitoring agent**: uses Genie pointed at `system.billing.usage` and
  `system.billing.list_prices` to answer spend questions in natural language

### Phase 3 — Zepz Readiness
- [ ] Document how to migrate from personal OAuth (U2M) to service principal (M2M) for production
- [ ] Write a short architecture brief on how Genie API agents reduce warehouse compute vs. ad-hoc
  Genie sessions — suitable for sharing with Drew / leadership
- [ ] Explore embedding Genie into a simple Slack bot or FastAPI web app

---

## Genie API Reference (Quick Sheet)

**Base URL**: `https://<DATABRICKS_HOST>/api/2.0/genie/spaces`

| Action | Method | Endpoint |
|---|---|---|
| List spaces | GET | `/api/2.0/genie/spaces` |
| Start conversation | POST | `/api/2.0/genie/spaces/{space_id}/start-conversation` |
| Send follow-up | POST | `/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages` |
| Get message status | GET | `/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}` |
| Get query result | GET | `/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}/query-result` |

**Auth header**: `Authorization: Bearer <token>`

**Recommended SDK approach** (preferred over raw REST):
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()  # picks up DATABRICKS_HOST + DATABRICKS_TOKEN from env
```

The SDK has a `w.genie` namespace that wraps the conversation API.

### Minimal API Flow
```python
# 1. Start a conversation
resp = w.genie.start_conversation(space_id=SPACE_ID, content="How many active users last month?")
conv_id = resp.conversation_id
msg_id = resp.message_id

# 2. Poll until complete
import time
while True:
    msg = w.genie.get_message(space_id=SPACE_ID, conversation_id=conv_id, message_id=msg_id)
    if msg.status in ("COMPLETED", "FAILED", "CANCELLED"):
        break
    time.sleep(2)

# 3. Retrieve result
result = w.genie.get_message_query_result(
    space_id=SPACE_ID, conversation_id=conv_id, message_id=msg_id
)
```

---

## Suggested Project Structure

```
genie-agent/
├── CLAUDE.md               ← this file
├── .env                    ← secrets (gitignored)
├── .gitignore
├── README.md
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── client.py           ← GenieCient wrapper (retry, polling, error handling)
│   ├── router.py           ← query router (simple vs. complex classification)
│   ├── trusted_queries.py  ← library of pre-approved parameterized SQL
│   ├── cost_monitor.py     ← Genie space for system.billing queries
│   └── cli.py              ← CLI entrypoint (argparse or Typer)
├── tests/
│   ├── test_client.py
│   └── test_router.py
└── notebooks/
    └── exploration.ipynb   ← scratch space for interactive testing
```

---

## Coding Standards

- **Python only** for all agent/API work
- Use `databricks-sdk` (not raw `requests`) wherever possible — it handles auth and retries cleanly
- All secrets via `python-dotenv` / environment variables — never hardcoded
- Type hints on all function signatures
- Docstrings on all public methods
- Retry logic on all API calls: exponential backoff, max 3 retries, respect 5 QPM limit
- Log all API calls and responses at DEBUG level (use `logging`, not `print`)
- Tests with `pytest`; aim for coverage on the client wrapper and router

---

## Key Dependencies

```
databricks-sdk>=0.25.0
python-dotenv
typer[all]          # CLI
rich                # pretty terminal output
pytest
pytest-mock
```

---

## First Session Checklist (Do This Tomorrow)

1. `mkdir genie-agent && cd genie-agent`
2. Create `venv`: `python -m venv .venv && source .venv/bin/activate`
3. `pip install databricks-sdk python-dotenv typer rich`
4. Create `.env` with your `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
5. In Databricks UI: create a Genie space, add a Unity Catalog table, add 3–5 example SQL queries
6. Copy the Genie space ID from the URL into `.env` as `GENIE_SPACE_ID`
7. Run a quick connectivity test:
   ```python
   from databricks.sdk import WorkspaceClient
   w = WorkspaceClient()
   spaces = list(w.genie.list_spaces())
   print(spaces)
   ```
8. If you see your space, the API is working — start building `client.py`

---

## Background Context for Claude Code Sessions

- This is a **personal learning/portfolio project**, not yet production code
- Zepz is a global money transfer company (formerly WorldRemit + Zepz) — a regulated fintech
- Drew Stooksberry is Mike's new manager; he mentioned Genie specifically, so demonstrating
  fluency with it early is a priority
- The cost-monitoring use case (Phase 2) is likely to resonate most strongly at Zepz given the
  cost-conscious culture Drew described
- Mike has an existing GitHub portfolio at `sherman94062.github.io` — completed work here may
  be added to that portfolio
- Mike is familiar with MCP, has built Databricks and other MCP servers, and may want to wrap
  the Genie client as an MCP tool in a future phase
