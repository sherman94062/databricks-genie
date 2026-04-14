"""CLI entrypoint: `python -m src.cli ...`"""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from .client import GenieClient
from .cost import CostReporter
from .router import QueryRouter
from .session_log import SessionLog

load_dotenv()

app = typer.Typer(help="Databricks Genie API agent CLI")
console = Console()


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@app.command()
def spaces(verbose: bool = typer.Option(False, "--verbose", "-v")) -> None:
    """List Genie spaces accessible to the current token."""
    _configure_logging(verbose)
    client = GenieClient(space_id=os.environ.get("GENIE_SPACE_ID", "placeholder"))
    table = Table("ID", "Title", "Description")
    for s in client.list_spaces():
        table.add_row(
            str(getattr(s, "space_id", "")),
            str(getattr(s, "title", "")),
            str(getattr(s, "description", "") or "")[:60],
        )
    console.print(table)


@app.command()
def ask(
    question: str = typer.Argument(..., help="Natural-language question"),
    space_id: Optional[str] = typer.Option(None, "--space", help="Override GENIE_SPACE_ID"),
    no_router: bool = typer.Option(False, "--no-router", help="Skip trusted-query routing"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Ask a natural-language question; routes to trusted SQL or Genie."""
    _configure_logging(verbose)

    if not no_router:
        decision = QueryRouter().route(question)
        if decision.route == "trusted" and decision.trusted:
            console.print(
                f"[green]Routed:[/green] {decision.reason}\n"
                f"[dim]Would execute trusted SQL:[/dim]\n{decision.trusted.sql}"
            )
            console.print(
                "[yellow]Note:[/yellow] trusted-query execution against SQL warehouse "
                "not yet wired up — Phase 2."
            )
            return

    client = GenieClient(space_id=space_id, session_log=SessionLog())
    console.print(f"[blue]Asking Genie:[/blue] {question}")
    result = client.ask(question)

    console.print(f"[bold]Status:[/bold] {result.status}")
    if result.content:
        console.print(f"[bold]Answer:[/bold] {result.content}")
    if result.sql:
        console.print(f"[bold]SQL:[/bold]\n{result.sql}")
    if result.columns:
        t = Table(*result.columns)
        for row in result.rows[:50]:
            t.add_row(*[str(c) for c in row])
        console.print(t)
        if len(result.rows) > 50:
            console.print(f"[dim]... {len(result.rows) - 50} more rows[/dim]")


@app.command()
def activity(limit: int = typer.Option(20, "--limit", "-n")) -> None:
    """Show recent Genie API activity from the local session log."""
    rows = SessionLog().recent(limit=limit)
    if not rows:
        console.print("[yellow]No activity logged yet.[/yellow] Run `ask` first.")
        return
    t = Table("when (UTC)", "latency", "status", "rows", "question")
    from datetime import datetime, timezone

    for r in rows:
        when = datetime.fromtimestamp(r["ts_start_utc"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        t.add_row(
            when,
            f"{r['latency_s']:.2f}s",
            r["status"],
            str(r["row_count"]),
            (r["question"] or "")[:60],
        )
    console.print(t)

    s = SessionLog().summary()
    console.print(
        f"[dim]Total calls: {s['n']} · avg latency {s['avg_latency_s']:.2f}s · "
        f"completed {s['completed']} · errors {s['errors']}[/dim]"
    )


@app.command()
def cost(
    hours: int = typer.Option(24, "--hours", "-h", help="Look back N hours"),
    breakdown: bool = typer.Option(False, "--breakdown", help="Per-day per-SKU detail"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Attribute warehouse spend to Genie activity using system.billing."""
    _configure_logging(verbose)
    import time as _t

    since_utc = _t.time() - hours * 3600
    reporter = CostReporter(session_log=SessionLog())

    if breakdown:
        console.print(f"[bold]Warehouse spend breakdown (last {hours}h):[/bold]")
        r = reporter.spend_breakdown_since(since_utc)
        if not r.rows:
            console.print("[yellow]No billing rows (billing has a lag — try --hours 48).[/yellow]")
            return
        t = Table(*r.columns)
        for row in r.rows:
            t.add_row(*[str(c) for c in row])
        console.print(t)
        return

    summary = reporter.attribute_to_session(since_utc=since_utc)
    t = Table("metric", "value")
    for k, v in summary.items():
        t.add_row(k, str(v))
    console.print(t)


def main() -> None:
    app()


if __name__ == "__main__":
    sys.exit(main() or 0)
