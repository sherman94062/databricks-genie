# CLAUDE.md — src/streaming

Scoped guide for Claude Code working in this directory. Complements the
repo-root `CLAUDE.md` (which covers the overall `databricks-genie`
project). Read the root file first for workspace wiring, env vars, and
the relationship to `src/monitors/`.

---

## What we're building

A **Structured Streaming compliance + fraud detector** over
`workspace.zepz.transactions`. It reads the transactions table as a
Delta stream, applies three detection rules in real time, writes
per-transaction findings to a new detail table, and writes a
per-batch summary row to the existing `workspace.zepz.audit_runs`
table so the whole history — batch monitors *and* streaming runs —
is queryable uniformly via MCP.

**Why it matters:**

The 2023 CFPB enforcement action against Sendwave (a Zepz brand) cited
misrepresented **fees**, **FX rates**, and **delivery times**. Our
source table exposes all three as paired disclosed/actual columns, so
we can detect these violations the moment a transaction lands in the
Lakehouse — not a day later when the batch marts run. That's the story
this module tells.

## Repo context (what already exists)

- **Root** — `databricks-genie`, a Databricks/UC tooling repo. Uses
  `databricks-sdk`, `typer`, `rich`, `python-dotenv`. Env vars come
  from `.env` (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`,
  `DATABRICKS_WAREHOUSE_ID`, `GENIE_SPACE_ID`).
- **`src/monitors/cfpb_compliance_monitor.py`** — sibling batch
  monitor. Writes to `workspace.zepz.audit_runs`. **Match its patterns
  for persistence, CLI flags, severity calc, and markdown output.**
- **`workspace.zepz.audit_runs`** — already exists (Delta, 8 cols).
  Schema and column meanings are in `src/monitors/schema.sql`.
- **`workspace.zepz.transactions`** — source table, 18 columns (see
  "Source schema" below). Static table today; we treat it as a stream.
- **`workspace.zepz_zepz_marts.*`** — dbt marts (batch). Not used here.

## Prerequisites

Before writing code, verify:

1. `.env` is populated and the repo-root `python -m src.cli spaces`
   connectivity test succeeds.
2. `workspace.zepz.audit_runs` exists (query
   `DESCRIBE TABLE workspace.zepz.audit_runs` via Databricks CLI or
   SQL editor).
3. Databricks CLI is installed and authenticated:
   `databricks auth login --host $DATABRICKS_HOST`.
4. User is on **Databricks Free Edition** — this constrains how we
   run the stream (see "Free Edition gotchas" below).

## Source schema — `workspace.zepz.transactions`

| Column | Type | Notes |
|---|---|---|
| `tx_id` | string | Primary key |
| `customer_id` | string | FK to `customers` |
| `corridor` | string | e.g. `US->PH` |
| `send_country`, `receive_country` | string | |
| `send_amount`, `receive_amount` | double | |
| `send_currency`, `receive_currency` | string | |
| `disclosed_fee` | double | **CFPB-disclosed fee** |
| `charged_fee` | double | **Actual fee charged** |
| `disclosed_fx_rate` | double | **CFPB-disclosed FX rate** |
| `applied_fx_rate` | double | **Actual FX rate used** |
| `transfer_method` | string | |
| `promised_delivery_ts` | **string** | ISO timestamp stored as string — cast with `to_timestamp()` |
| `actual_delivery_ts` | **string** | Same — cast with `to_timestamp()` |
| `status` | string | `completed`, `pending`, `failed`, etc. |
| `created_at` | **string** | Event time. Cast for watermarking. |

The three string-typed timestamps are deliberate gotchas — you'll cast
them before any time-based logic.

## Target architecture

```
workspace.zepz.transactions  (Delta source, read as stream)
         │
         ├──> Rule 1: fee disclosure mismatch   (per-row filter)
         ├──> Rule 2: FX rate disclosure mismatch (per-row filter)
         └──> Rule 3: customer velocity anomaly (windowed aggregation)
                                │
                                ▼
         workspace.zepz.stream_findings (new Delta table, detail rows)
                                │
                                ▼
         workspace.zepz.audit_runs (existing, one summary row per run)
```

Both sinks share the `run_id` / `run_ts` pattern with the batch
monitor, so Claude (via MCP) can answer questions that span batch and
stream history without special-casing.

## Files to create

```
src/streaming/
├── __init__.py
├── CLAUDE.md                    # (this file, already present)
├── README.md                    # User-facing docs
├── schema.sql                   # DDL for stream_findings
├── notebook_fraud_detector.py   # Databricks notebook (Python cells)
├── run_detector.py              # Local CLI wrapper (SDK-driven)
└── tests/
    └── test_rules.py            # Pure-Python tests of rule predicates
```

## Implementation details

### `schema.sql`

Creates `workspace.zepz.stream_findings`. One row per flagged
transaction. Columns:

```sql
CREATE TABLE IF NOT EXISTS workspace.zepz.stream_findings (
  run_id         STRING    COMMENT 'Links to audit_runs.run_id',
  run_ts         TIMESTAMP COMMENT 'When the streaming job started',
  tx_id          STRING    COMMENT 'Transaction that triggered the finding',
  customer_id    STRING,
  corridor       STRING,
  rule_code      STRING    COMMENT 'fee_mismatch | fx_mismatch | velocity_anomaly',
  severity       STRING    COMMENT 'warn | fail',
  observed_value DOUBLE    COMMENT 'The value that tripped the rule (pct delta, count, etc.)',
  threshold      DOUBLE    COMMENT 'Threshold the value exceeded',
  detail         STRING    COMMENT 'Human-readable finding text',
  detected_at    TIMESTAMP COMMENT 'When this finding was written'
)
USING DELTA
COMMENT 'Per-transaction findings from the streaming compliance detector.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

### `notebook_fraud_detector.py` — the core detector

A Databricks notebook in pure-Python format (cells separated by
`# COMMAND ----------`). Runs on a Databricks cluster / serverless,
not locally. Order of cells:

1. **Params** — widgets for `run_id`, `mode` (`dry_run` or `write`),
   `checkpoint_root`. Defaults work out of the box.
2. **Imports + config** — `pyspark.sql.functions as F`, constants for
   catalog/schema/table names, thresholds dict.
3. **Source stream** — `spark.readStream.table("workspace.zepz.transactions")`
   then derive an `event_ts` column with `F.to_timestamp("created_at")`.
4. **Rule 1: fee_mismatch** — per-row, no windowing. Flag rows where
   `charged_fee > disclosed_fee * (1 + FEE_TOLERANCE)`. Default
   tolerance = `0.05`. Produce a DataFrame matching the
   `stream_findings` schema with `rule_code = 'fee_mismatch'`,
   `severity = 'fail'`.
5. **Rule 2: fx_mismatch** — per-row. Flag rows where
   `applied_fx_rate < disclosed_fx_rate * (1 - FX_TOLERANCE)` and
   `disclosed_fx_rate > 0`. Default tolerance = `0.02`.
   `rule_code = 'fx_mismatch'`, `severity = 'fail'`.
6. **Rule 3: velocity_anomaly** — windowed. Watermark of 10 minutes
   on `event_ts`. 1-hour tumbling window grouped by `customer_id`.
   Flag groups where `count(*) > 5` OR `sum(send_amount) > 10000`.
   Explode the window rows back to per-tx findings by joining to the
   underlying rows in the same window.
   `rule_code = 'velocity_anomaly'`, `severity = 'warn'`.
7. **Union** — `unionByName` the three finding DataFrames, add
   `run_id`, `run_ts`, `detected_at`.
8. **Write stream** — single `writeStream` with
   `trigger(availableNow=True)`, `checkpointLocation` under
   `{checkpoint_root}/{run_id}/findings`, write to
   `workspace.zepz.stream_findings` via `toTable()`.
   **Wait for termination** before proceeding.
9. **Summary write** — after the stream terminates, read back from
   `stream_findings` filtered by `run_id`, aggregate counts by
   `rule_code`, build a markdown summary string, and INSERT a single
   row into `workspace.zepz.audit_runs` with:
   - `monitor_name = 'streaming_compliance_detector'`
   - `severity = 'fail'` if any `fail`-severity findings, else
     `'warn'` if any warn findings, else `'ok'`
   - `checks_total = 3` (one per rule)
   - `checks_failed` = number of rules that produced at least one finding
   - `findings` = JSON array `[{rule_code, count, severity}]`
   - `summary_md` = markdown summary string

### `run_detector.py` — local CLI wrapper

`typer` app that matches `src/monitors/cfpb_compliance_monitor.py`
style. Commands:

- `run_detector.py deploy` — upload `notebook_fraud_detector.py` to a
  workspace path (e.g. `/Workspace/Users/{me}/databricks-genie/streaming`)
  using the Databricks SDK's `workspace.import_` with format `SOURCE`.
- `run_detector.py run --mode {dry_run|write}` — trigger a one-time
  job run against the uploaded notebook via
  `w.jobs.submit(...)` with a `NotebookTask`, then poll
  `w.jobs.get_run(...)` until terminal state. Print run URL.
- `run_detector.py tail --run-id {id}` — read the latest `audit_runs`
  row and `stream_findings` count for a run id; print markdown.
- Global flags: `--host`, `--token`, `--cluster-id` (optional — if
  omitted, use the workspace's default serverless). Env-var fallback
  via `load_dotenv`.

Exit codes: 0 ok/warn, 2 fail — match the batch monitor.

### `tests/test_rules.py`

Pure-Python (no Spark session needed) unit tests of the rule
predicates. Extract the per-row rule conditions into small functions:

```python
def is_fee_mismatch(charged_fee: float, disclosed_fee: float,
                    tolerance: float = 0.05) -> bool: ...
def is_fx_mismatch(applied: float, disclosed: float,
                   tolerance: float = 0.02) -> bool: ...
```

Then test happy/edge cases (exactly at threshold, zero disclosed,
negative tolerance). Velocity is harder to unit-test without Spark;
skip it here, test via an integration run.

## Running it

```bash
# One-time table creation (run in Databricks SQL editor):
#   paste contents of src/streaming/schema.sql

# Local iteration
cd ~/path/to/databricks-genie
source .venv/bin/activate

# Tests first
pytest src/streaming/tests/ -v

# Upload the notebook to the workspace
python -m src.streaming.run_detector deploy

# Dry run (reads only, no writes)
python -m src.streaming.run_detector run --mode dry_run

# Real run (writes to stream_findings + audit_runs)
python -m src.streaming.run_detector run --mode write

# Read back the result
python -m src.streaming.run_detector tail --run-id <uuid-from-run>
```

After a successful `write` run, ask Claude via MCP:

- *"How many stream_findings did the last detector run produce, by rule_code?"*
- *"Show me the audit_runs entries from the streaming detector over the last week."*

## Free Edition gotchas

These trip everyone the first time:

1. **Use `trigger(availableNow=True)`** — Free Edition doesn't
   support long-running streaming jobs on dedicated compute. With
   `availableNow`, the stream processes all currently-available data
   and terminates — gives you streaming *primitives* (watermarks,
   stateful windowing, checkpoints) without a long-running cluster.
2. **No Unity Catalog Volumes on Free Edition** (as of writing).
   Use DBFS for checkpoints: `dbfs:/tmp/databricks-genie/checkpoints/{run_id}/`.
   Verify with `w.dbfs.list("/tmp")` before assuming the path.
3. **Serverless compute only.** `run_detector.py` should not require
   a `cluster_id` — submit the job with serverless compute by
   omitting the compute spec or specifying
   `NotebookTask(...notebook_path=...)` with
   `JobEnvironment` defaults.
4. **Checkpoints are per-run.** New `run_id` every run → new
   checkpoint dir. This avoids "stream already exists" errors when
   iterating. For production you'd keep one checkpoint; we don't here.
5. **String timestamps everywhere.** `created_at`,
   `promised_delivery_ts`, `actual_delivery_ts` are all stored as
   strings. Always `F.to_timestamp(col)` before using them with
   watermarks, windowing, or comparisons.
6. **`.readStream.table(...)` needs Delta source**, which this is.
   No `.option("readChangeFeed", "true")` needed for v1; we're
   processing the full table on each run, which is fine for
   `availableNow` demos.

## Definition of done

- [ ] `schema.sql` executed; `stream_findings` visible in
      `workspace.zepz`.
- [ ] `pytest src/streaming/tests/` passes.
- [ ] `run_detector deploy` uploads the notebook; visible in the
      workspace UI under `/Workspace/Users/{me}/databricks-genie/streaming`.
- [ ] `run_detector run --mode dry_run` completes and prints the
      count of rows that *would* be flagged per rule, without writing.
- [ ] `run_detector run --mode write` populates `stream_findings`
      with > 0 rows (the current table has known disclosure
      mismatches) and adds exactly one row to `audit_runs` with
      `monitor_name = 'streaming_compliance_detector'`.
- [ ] Claude via MCP can answer: *"Which rule fired most in the last
      streaming run?"* correctly.
- [ ] README.md in this directory covers setup, run, troubleshoot.

## Out of scope for v1 (stretch)

- High-value / structuring rule
  (`send_amount` between $8,500 and $9,999 — just under the $10k
  CTR). Add as Rule 4 in a follow-up.
- Delivery SLA breach rule (compare `actual_delivery_ts` vs
  `promised_delivery_ts`). Interesting but needs careful null handling.
- DLT / Lakeflow Declarative Pipelines version. Natural follow-up
  for Week 4 once we've mastered raw Structured Streaming.
- Slack/webhook alerting on `fail`-severity runs. Do after the
  batch monitor gets the same treatment, so they share one notifier.
- Grafana / Databricks AI/BI dashboard reading `stream_findings`.
  Good Streamlit add to `databricks-genie` after `audit_runs` has
  enough history to chart.

## Design principles (keep these in mind)

- **Rule code is data, not Python.** Each rule's thresholds live in a
  single `THRESHOLDS` dict at the top of the notebook. Adding a new
  rule should mean adding one function + one dict entry + a union.
- **Shared schema across monitors.** `audit_runs` is the one surface
  Claude queries. If your streaming summary doesn't fit the existing
  8 columns, make it fit — don't add columns.
- **Readability over cleverness.** The notebook will be opened by a
  Zepz teammate who's never seen it. Comment the watermark, comment
  the windowing, comment the severity mapping. Every `F.xxx()` call
  with a non-obvious argument gets a `#` line above it.
- **Pure-Python tests for rule predicates.** Spark session setup
  takes 30s; extracting the math into testable functions is a
  15-minute effort that pays back every time you tune a threshold.
- **Fail loudly.** If `disclosed_fx_rate` is zero or null, that's a
  data-quality problem — don't silently skip, emit a finding with
  `rule_code = 'fx_data_quality'` so it surfaces in the summary.
  (Add this only if straightforward; v1 can just filter nulls.)

## When in doubt

- Match the style of `src/monitors/cfpb_compliance_monitor.py`.
- Read the root `CLAUDE.md` before introducing new dependencies.
- If a Free Edition limitation blocks you, document it in README.md
  and fall back to the simplest working approach — don't invent
  workarounds that break on real clusters.
