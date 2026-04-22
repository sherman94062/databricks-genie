-- One-time DDL for the CFPB Compliance Monitor audit log.
-- Run this once in the Databricks SQL editor or via the CLI:
--   databricks sql --warehouse-id <id> -q "$(cat src/monitors/schema.sql)"

CREATE TABLE IF NOT EXISTS workspace.zepz.audit_runs (
  run_id        STRING  COMMENT 'UUID for this monitor run',
  run_ts        TIMESTAMP COMMENT 'When the monitor executed (UTC)',
  monitor_name  STRING  COMMENT 'Which monitor produced this row',
  severity      STRING  COMMENT 'Overall severity: ok | warn | fail',
  checks_total  INT     COMMENT 'Total checks evaluated this run',
  checks_failed INT     COMMENT 'Checks that flagged warn or fail',
  findings      STRING  COMMENT 'JSON array of per-check finding objects',
  summary_md    STRING  COMMENT 'Human-readable markdown summary'
)
USING DELTA
COMMENT 'Execution history for zepz compliance monitors. Queried by Claude via MCP for natural-language audit questions.';
