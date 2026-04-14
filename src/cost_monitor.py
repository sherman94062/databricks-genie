"""Cost-monitoring agent: thin wrapper around GenieClient pointed at a
Genie space that's been configured over `system.billing.usage` and
`system.billing.list_prices`.

Phase 2 deliverable — placeholder until a billing-scoped Genie space exists.
"""

from __future__ import annotations

import os
from typing import Optional

from .client import GenieClient, GenieResult


class CostMonitor:
    def __init__(self, space_id: Optional[str] = None):
        self.space_id = space_id or os.environ.get("GENIE_COST_SPACE_ID") or os.environ.get(
            "GENIE_SPACE_ID"
        )
        if not self.space_id:
            raise ValueError("GENIE_COST_SPACE_ID or GENIE_SPACE_ID env var required")
        self.client = GenieClient(space_id=self.space_id)

    def ask(self, question: str) -> GenieResult:
        return self.client.ask(question)

    def spend_last_30d(self) -> GenieResult:
        return self.ask("What was our total DBU spend over the last 30 days?")

    def top_skus(self, n: int = 10) -> GenieResult:
        return self.ask(f"What are the top {n} SKUs by spend in the last 30 days?")
