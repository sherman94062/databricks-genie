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
from .router import QueryRouter

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

    client = GenieClient(space_id=space_id)
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


def main() -> None:
    app()


if __name__ == "__main__":
    sys.exit(main() or 0)
