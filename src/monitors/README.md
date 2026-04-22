# src/monitors

Daily monitors over the zepz marts in `workspace.zepz_zepz_marts`. Each
monitor applies a set of threshold checks, writes a row to
`workspace.zepz.audit_runs`, and emits a markdown summary. Findings are
queryable by Claude via MCP for natural-language compliance questions.

## cfpb_compliance_monitor

CFPB-adjacent checks against `rpt_dashboard_kpis`, `rpt_corridor_summary`,
and `rpt_rule_summary`:

| Check | What it catches |
|---|---|
| `freshness` | `dbt_loaded_at` older than 24h (warn) or 72h (fail) |
| `violation_rate` | Share of transactions with any violation > 40% warn / 50% fail |
| `critical_count` | Absolute critical-severity count > 500 warn / 1000 fail |
| `total_exposure` | Dollar exposure from violated tx > $500K warn / $1M fail |
| `corridor_concentration` | Any single corridor > 25% of all violations |
| `rule_concentration` | Any single rule code > 40% of all violations |

Thresholds live in `THRESHOLDS` at the top of
`cfpb_compliance_monitor.py`. Tune to your production distribution.

## Setup (one-time)

Run the DDL once to create the audit table:

```bash
# Option 1: paste src/monitors/schema.sql into the Databricks SQL editor
# Option 2: via the Databricks CLI
databricks sql --warehouse-id $DATABRICKS_WAREHOUSE_ID \
    -q "$(cat src/monitors/schema.sql)"
```

## Running locally

```bash
cp .env.example .env   # fill in DATABRICKS_HOST / TOKEN / WAREHOUSE_ID
source .venv/bin/activate
pip install -r requirements.txt

# Dry run — prints findings, does NOT write to audit_runs
python -m src.monitors.cfpb_compliance_monitor --dry-run

# Real run — writes a row to workspace.zepz.audit_runs
python -m src.monitors.cfpb_compliance_monitor
```

Exit codes: `0` for ok/warn, `2` for fail — convenient for cron, CI, or
Databricks Workflows with retry policies.

## Running as a Databricks Workflow

1. Sync the repo to a Databricks Repo (`/Repos/you/databricks-genie`).
2. Create a Workflow with a **Python Script** task pointing at
   `src/monitors/cfpb_compliance_monitor.py`.
3. Schedule daily at your chosen UTC hour.
4. Pass `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and
   `DATABRICKS_WAREHOUSE_ID` as task env vars, or switch to service
   principal OAuth by dropping `token` from the `WorkspaceClient()` call.

## Querying results via Claude

With the Databricks MCP server connected, Claude can answer:

- *"Did the compliance monitor pass today?"*
- *"Show me the last failed run and what triggered it."*
- *"Which check has failed most often in the last 30 days?"*
- *"Plot the violation_rate finding value over the last 7 runs."*

...by reading `workspace.zepz.audit_runs` directly.

## Adding a new check

1. Write a function that takes a `Sql` helper and returns a `Finding`
   (or `list[Finding]`).
2. Call it from `main()` between the existing checks.
3. Add a row to the threshold table above.

That's it — persistence and formatting handle the rest.
