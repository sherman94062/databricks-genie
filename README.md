# databricks-genie

Tooling around the Databricks AI/BI Genie Conversation API — client wrapper, query router, and cost-monitoring agent.

See `CLAUDE.md` for full project context and phased goals.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in DATABRICKS_HOST, DATABRICKS_TOKEN, GENIE_SPACE_ID
```

## Quick connectivity test

```bash
python -m src.cli spaces
```

## Ask Genie a question

```bash
python -m src.cli ask "How many rows are in the sample table?"
```

## Run tests

```bash
pytest
```
