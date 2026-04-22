"""CFPB Compliance Monitor.

Daily job that scans the zepz marts, flags anomalies against
compliance thresholds, writes findings to
``workspace.zepz.audit_runs``, and emits a markdown summary.

See ``src/monitors/README.md`` for usage.
"""
from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import typer
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    StatementParameterListItem,
    StatementState,
)
from dotenv import load_dotenv
from rich.console import Console
from rich.markdown import Markdown

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CATALOG = "workspace"
MARTS_SCHEMA = "zepz_zepz_marts"
AUDIT_SCHEMA = "zepz"
AUDIT_TABLE = "audit_runs"
MONITOR_NAME = "cfpb_compliance_monitor"

# Thresholds — tune against production violation distribution.
THRESHOLDS: dict[str, float] = {
    "freshness_hours_warn": 24,
    "freshness_hours_fail": 72,
    "violation_rate_warn_pct": 40.0,
    "violation_rate_fail_pct": 50.0,
    "critical_count_warn": 500,
    "critical_count_fail": 1000,
    "exposure_warn_usd": 500_000,
    "exposure_fail_usd": 1_000_000,
    "corridor_share_warn_pct": 25.0,
    "rule_concentration_warn_pct": 40.0,
}

SEV_EMOJI = {"ok": "\u2705", "warn": "\u26a0\ufe0f", "fail": "\U0001f6a8"}

console = Console()
app = typer.Typer(add_completion=False, help=__doc__)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Finding:
    check_id: str
    severity: str  # "ok" | "warn" | "fail"
    title: str
    value: Any
    threshold: Any
    detail: str


@dataclass
class RunResult:
    run_id: str
    run_ts: datetime
    findings: list[Finding] = field(default_factory=list)

    @property
    def checks_total(self) -> int:
        return len(self.findings)

    @property
    def checks_failed(self) -> int:
        return sum(1 for f in self.findings if f.severity != "ok")

    @property
    def severity(self) -> str:
        if any(f.severity == "fail" for f in self.findings):
            return "fail"
        if any(f.severity == "warn" for f in self.findings):
            return "warn"
        return "ok"


# ---------------------------------------------------------------------------
# SQL helper
# ---------------------------------------------------------------------------

class Sql:
    """Thin wrapper around ``WorkspaceClient.statement_execution``."""

    def __init__(self, client: WorkspaceClient, warehouse_id: str) -> None:
        self.client = client
        self.warehouse_id = warehouse_id

    def _exec(
        self,
        statement: str,
        parameters: list[StatementParameterListItem] | None = None,
    ):
        resp = self.client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=statement,
            parameters=parameters,
            wait_timeout="30s",
        )
        if resp.status and resp.status.state != StatementState.SUCCEEDED:
            err = resp.status.error.message if resp.status.error else "unknown error"
            raise RuntimeError(f"Statement failed: {err}\n--- SQL ---\n{statement}")
        return resp

    def fetch_one(self, query: str) -> tuple[Any, ...]:
        resp = self._exec(query)
        rows = resp.result.data_array if resp.result else []
        if not rows:
            raise RuntimeError("Query returned no rows")
        return tuple(rows[0])

    def fetch_all(self, query: str) -> list[tuple[Any, ...]]:
        resp = self._exec(query)
        rows = resp.result.data_array if resp.result else []
        return [tuple(row) for row in rows]

    def execute(
        self,
        statement: str,
        parameters: list[StatementParameterListItem] | None = None,
    ) -> None:
        self._exec(statement, parameters)


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def _severity(value: float, warn: float, fail: float) -> str:
    if value >= fail:
        return "fail"
    if value >= warn:
        return "warn"
    return "ok"


def _parse_timestamp(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        dt = raw
    else:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def check_freshness(sql: Sql) -> Finding:
    (loaded_at_raw,) = sql.fetch_one(
        f"SELECT dbt_loaded_at FROM {CATALOG}.{MARTS_SCHEMA}.rpt_dashboard_kpis"
    )
    loaded_at = _parse_timestamp(loaded_at_raw)
    age_h = (datetime.now(timezone.utc) - loaded_at).total_seconds() / 3600
    return Finding(
        check_id="freshness",
        severity=_severity(
            age_h,
            THRESHOLDS["freshness_hours_warn"],
            THRESHOLDS["freshness_hours_fail"],
        ),
        title="Marts data freshness",
        value=round(age_h, 1),
        threshold=THRESHOLDS["freshness_hours_warn"],
        detail=f"dbt_loaded_at = {loaded_at.isoformat()}, age = {age_h:.1f}h",
    )


def check_kpis(sql: Sql) -> list[Finding]:
    row = sql.fetch_one(f"""
        SELECT violation_rate_pct, critical_count, total_exposure
        FROM {CATALOG}.{MARTS_SCHEMA}.rpt_dashboard_kpis
    """)
    rate, critical, exposure = float(row[0]), int(row[1]), int(row[2])
    return [
        Finding(
            check_id="violation_rate",
            severity=_severity(
                rate,
                THRESHOLDS["violation_rate_warn_pct"],
                THRESHOLDS["violation_rate_fail_pct"],
            ),
            title="Violation rate",
            value=rate,
            threshold=THRESHOLDS["violation_rate_warn_pct"],
            detail=f"{rate:.1f}% of transactions have at least one violation",
        ),
        Finding(
            check_id="critical_count",
            severity=_severity(
                critical,
                THRESHOLDS["critical_count_warn"],
                THRESHOLDS["critical_count_fail"],
            ),
            title="Critical violation count",
            value=critical,
            threshold=THRESHOLDS["critical_count_warn"],
            detail=f"{critical:,} critical-severity violations observed",
        ),
        Finding(
            check_id="total_exposure",
            severity=_severity(
                exposure,
                THRESHOLDS["exposure_warn_usd"],
                THRESHOLDS["exposure_fail_usd"],
            ),
            title="Total compliance exposure",
            value=exposure,
            threshold=THRESHOLDS["exposure_warn_usd"],
            detail=f"${exposure:,} at risk from violated transactions",
        ),
    ]


def check_corridor_concentration(sql: Sql) -> Finding:
    rows = sql.fetch_all(f"""
        SELECT corridor,
               violation_count,
               ROUND(100.0 * violation_count /
                     SUM(violation_count) OVER (), 2) AS share_pct
        FROM {CATALOG}.{MARTS_SCHEMA}.rpt_corridor_summary
        ORDER BY violation_count DESC
        LIMIT 1
    """)
    corridor, count, share_raw = rows[0]
    share = float(share_raw)
    return Finding(
        check_id="corridor_concentration",
        severity="warn" if share > THRESHOLDS["corridor_share_warn_pct"] else "ok",
        title="Top corridor violation share",
        value=share,
        threshold=THRESHOLDS["corridor_share_warn_pct"],
        detail=f"{corridor} accounts for {share:.1f}% of violations ({count} events)",
    )


def check_rule_concentration(sql: Sql) -> Finding:
    rows = sql.fetch_all(f"""
        SELECT rule_code,
               violation_count,
               ROUND(100.0 * violation_count /
                     SUM(violation_count) OVER (), 2) AS share_pct
        FROM {CATALOG}.{MARTS_SCHEMA}.rpt_rule_summary
        ORDER BY violation_count DESC
        LIMIT 1
    """)
    rule, count, share_raw = rows[0]
    share = float(share_raw)
    return Finding(
        check_id="rule_concentration",
        severity="warn" if share > THRESHOLDS["rule_concentration_warn_pct"] else "ok",
        title="Top rule concentration",
        value=share,
        threshold=THRESHOLDS["rule_concentration_warn_pct"],
        detail=f"Rule {rule} triggers {share:.1f}% of violations ({count} events)",
    )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist(sql: Sql, result: RunResult, summary_md: str) -> None:
    findings_json = json.dumps([asdict(f) for f in result.findings])
    params = [
        StatementParameterListItem(name="run_id", value=result.run_id, type="STRING"),
        StatementParameterListItem(
            name="run_ts",
            value=result.run_ts.strftime("%Y-%m-%d %H:%M:%S"),
            type="TIMESTAMP",
        ),
        StatementParameterListItem(name="monitor_name", value=MONITOR_NAME, type="STRING"),
        StatementParameterListItem(name="severity", value=result.severity, type="STRING"),
        StatementParameterListItem(name="checks_total", value=str(result.checks_total), type="INT"),
        StatementParameterListItem(name="checks_failed", value=str(result.checks_failed), type="INT"),
        StatementParameterListItem(name="findings", value=findings_json, type="STRING"),
        StatementParameterListItem(name="summary_md", value=summary_md, type="STRING"),
    ]
    sql.execute(
        f"""
        INSERT INTO {CATALOG}.{AUDIT_SCHEMA}.{AUDIT_TABLE}
        VALUES (:run_id, :run_ts, :monitor_name, :severity,
                :checks_total, :checks_failed, :findings, :summary_md)
        """,
        parameters=params,
    )


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def format_markdown(result: RunResult) -> str:
    lines = [
        f"# CFPB Compliance Monitor \u2014 {result.run_ts.strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        f"**Overall: {SEV_EMOJI[result.severity]} {result.severity.upper()}**  ",
        f"Run ID: `{result.run_id}`  ",
        f"Checks: {result.checks_total} total, {result.checks_failed} flagged",
        "",
        "## Findings",
        "",
    ]
    for f in result.findings:
        lines.append(f"### {SEV_EMOJI[f.severity]} {f.title}")
        lines.append("")
        lines.append(f"- **Severity:** `{f.severity}`")
        lines.append(f"- **Value:** `{f.value}` (threshold: `{f.threshold}`)")
        lines.append(f"- {f.detail}")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

@app.command()
def main(
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print findings without writing to audit_runs."
    ),
    host: str = typer.Option(
        None, "--host", envvar="DATABRICKS_HOST",
        help="Databricks workspace host (no https://).",
    ),
    token: str = typer.Option(
        None, "--token", envvar="DATABRICKS_TOKEN",
        help="Databricks personal access token.",
    ),
    warehouse_id: str = typer.Option(
        None, "--warehouse-id", envvar="DATABRICKS_WAREHOUSE_ID",
        help="SQL warehouse ID (the value after /warehouses/ in the HTTP path).",
    ),
) -> None:
    """Run the CFPB compliance monitor and write findings to audit_runs."""
    load_dotenv()
    host = host or os.environ.get("DATABRICKS_HOST")
    token = token or os.environ.get("DATABRICKS_TOKEN")
    warehouse_id = warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID")

    if not all([host, token, warehouse_id]):
        console.print(
            "[red]DATABRICKS_HOST, DATABRICKS_TOKEN, and "
            "DATABRICKS_WAREHOUSE_ID are required.[/red]"
        )
        raise typer.Exit(code=1)

    client = WorkspaceClient(host=f"https://{host}", token=token)
    sql = Sql(client, warehouse_id)

    result = RunResult(
        run_id=str(uuid.uuid4()),
        run_ts=datetime.now(timezone.utc),
    )
    result.findings.append(check_freshness(sql))
    result.findings.extend(check_kpis(sql))
    result.findings.append(check_corridor_concentration(sql))
    result.findings.append(check_rule_concentration(sql))

    summary_md = format_markdown(result)
    console.print(Markdown(summary_md))

    if dry_run:
        console.print("[yellow]Dry run \u2014 audit_runs not written.[/yellow]")
    else:
        persist(sql, result, summary_md)
        console.print(
            f"[green]Wrote run {result.run_id} to "
            f"{CATALOG}.{AUDIT_SCHEMA}.{AUDIT_TABLE}[/green]"
        )

    raise typer.Exit(code={"ok": 0, "warn": 0, "fail": 2}[result.severity])


if __name__ == "__main__":
    app()
