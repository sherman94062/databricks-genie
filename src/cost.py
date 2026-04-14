"""Cost attribution for Genie API activity.

Executes SQL against `system.billing.usage` and `system.billing.list_prices`
on the configured warehouse, filtered by warehouse_id and the time windows
recorded in the local session log.

Billing data has latency — recent activity (last few hours) may not yet
be attributable. The billing tables live in the `system.billing` schema,
which requires the caller to have been granted read access.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

from .session_log import SessionLog

log = logging.getLogger(__name__)

USAGE_BY_WAREHOUSE_SQL = """
SELECT
  u.usage_date,
  u.sku_name,
  SUM(u.usage_quantity) AS dbus,
  SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) AS est_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON p.sku_name = u.sku_name
  AND u.usage_end_time >= p.price_start_time
  AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE u.usage_metadata.warehouse_id = :warehouse_id
  AND u.usage_start_time >= :since_ts
GROUP BY u.usage_date, u.sku_name
ORDER BY u.usage_date DESC, dbus DESC
"""

QUERY_HISTORY_BY_STATEMENT_IDS_SQL = """
SELECT
  statement_id,
  start_time,
  end_time,
  total_duration_ms,
  execution_duration_ms,
  read_rows,
  produced_rows,
  read_bytes,
  compute.warehouse_id AS warehouse_id,
  statement_text
FROM system.query.history
WHERE statement_id IN ({placeholders})
ORDER BY start_time DESC
"""

WAREHOUSE_TOTAL_SINCE_SQL = """
SELECT
  SUM(u.usage_quantity) AS dbus,
  SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) AS est_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON p.sku_name = u.sku_name
  AND u.usage_end_time >= p.price_start_time
  AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE u.usage_metadata.warehouse_id = :warehouse_id
  AND u.usage_start_time >= :since_ts
"""


@dataclass
class CostRow:
    columns: list[str]
    rows: list[list[Any]]


class CostReporter:
    def __init__(
        self,
        warehouse_id: Optional[str] = None,
        workspace: Optional[WorkspaceClient] = None,
        session_log: Optional[SessionLog] = None,
    ):
        self.warehouse_id = warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID")
        if not self.warehouse_id:
            raise ValueError("warehouse_id or DATABRICKS_WAREHOUSE_ID env var required")
        self.w = workspace or WorkspaceClient()
        self.session_log = session_log or SessionLog()

    def _execute(self, sql: str, params: dict[str, Any]) -> CostRow:
        parameters = [
            StatementParameterListItem(name=k, value=str(v)) for k, v in params.items()
        ]
        stmt = self.w.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=sql,
            parameters=parameters,
            wait_timeout="30s",
        )
        sid = stmt.statement_id
        while True:
            state = str(getattr(stmt.status, "state", "")).upper()
            if any(t in state for t in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED")):
                break
            time.sleep(1.0)
            stmt = self.w.statement_execution.get_statement(sid)  # type: ignore[arg-type]
        state = str(getattr(stmt.status, "state", "")).upper()
        if "SUCCEEDED" not in state:
            err = getattr(stmt.status, "error", None)
            raise RuntimeError(f"Statement {sid} ended in state {state}: {err}")

        manifest = getattr(stmt, "manifest", None)
        schema = getattr(manifest, "schema", None) if manifest else None
        cols = [c.name for c in getattr(schema, "columns", []) or []] if schema else []
        data = getattr(stmt, "result", None)
        rows = getattr(data, "data_array", None) or [] if data else []
        return CostRow(columns=cols, rows=rows)

    def per_statement_history(self, statement_ids: list[str]) -> CostRow:
        """Look up query history for specific statement_ids via the Query History API.

        Uses the workspace-level `query_history.list` endpoint (no system-table
        grants required) and filters client-side by statement_id.
        """
        if not statement_ids:
            return CostRow(columns=[], rows=[])
        wanted = set(statement_ids)

        cols = [
            "statement_id", "status", "query_start_time_ms", "duration_ms",
            "rows_produced", "user_name", "statement_type", "query_text",
        ]
        rows: list[list[Any]] = []
        try:
            iterator = self.w.query_history.list()
        except Exception as e:
            raise RuntimeError(f"query_history.list() failed: {e}") from e

        scanned = 0
        for q in iterator:
            scanned += 1
            qid = getattr(q, "query_id", None) or getattr(q, "statement_id", None)
            if qid in wanted:
                rows.append([
                    qid,
                    str(getattr(q, "status", "")),
                    getattr(q, "query_start_time_ms", None),
                    getattr(q, "duration", None) or getattr(q, "execution_end_time_ms", None),
                    getattr(q, "rows_produced", None),
                    getattr(q, "user_name", None),
                    str(getattr(q, "statement_type", "")),
                    (getattr(q, "query_text", "") or "")[:120],
                ])
                if len(rows) == len(wanted):
                    break
            if scanned > 1000:
                break
        return CostRow(columns=cols, rows=rows)

    def warehouse_spend_since(self, since_utc: float) -> CostRow:
        """Total DBUs + est. USD on this warehouse since `since_utc`."""
        return self._execute(
            WAREHOUSE_TOTAL_SINCE_SQL,
            {"warehouse_id": self.warehouse_id, "since_ts": _iso(since_utc)},
        )

    def spend_breakdown_since(self, since_utc: float) -> CostRow:
        """Per-day per-SKU spend on this warehouse since `since_utc`."""
        return self._execute(
            USAGE_BY_WAREHOUSE_SQL,
            {"warehouse_id": self.warehouse_id, "since_ts": _iso(since_utc)},
        )

    def attribute_to_session(self, since_utc: Optional[float] = None) -> dict:
        """Combine local session log with warehouse billing to estimate cost per Genie call.

        Strategy: total warehouse DBU × (sum(Genie call durations) / total active seconds)
        is a coarse proxy. For Free Edition serverless this is rough — the warehouse
        autoscales and bills per-second-of-query-time, so Genie-attributable cost ≈
        (dbu_rate_per_sec) × (sum of our call durations). We report both.
        """
        since = since_utc or (time.time() - 24 * 3600)
        windows = self.session_log.time_windows(since_utc=since)
        total_call_seconds = sum(max(0.0, e - s) for s, e in windows)
        call_count = len(windows)

        spend = self.warehouse_spend_since(since)
        dbus = float(spend.rows[0][0] or 0) if spend.rows else 0.0
        est_usd = float(spend.rows[0][1] or 0) if spend.rows else 0.0

        return {
            "since_utc": since,
            "warehouse_id": self.warehouse_id,
            "genie_calls": call_count,
            "genie_call_seconds": round(total_call_seconds, 2),
            "warehouse_dbus": dbus,
            "warehouse_est_usd": est_usd,
            "avg_seconds_per_call": round(total_call_seconds / call_count, 2) if call_count else 0,
            "avg_usd_per_call_estimate": round(est_usd / call_count, 4) if call_count else 0,
            "note": (
                "warehouse_* figures are TOTAL warehouse spend since `since_utc`, "
                "not just Genie. avg_usd_per_call is a naive evenly-split estimate."
            ),
        }


def _iso(ts: float) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
