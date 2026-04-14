"""Query router: decide whether a question can be served by a trusted
pre-canned query (cheap) or should be sent to Genie (expensive).

Keeps warehouse compute down by short-circuiting high-frequency questions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Optional

from .trusted_queries import TRUSTED_QUERIES, TrustedQuery


@dataclass
class RouteDecision:
    route: str  # "trusted" | "genie"
    trusted: Optional[TrustedQuery] = None
    reason: str = ""


class QueryRouter:
    def __init__(self, trusted: Optional[list[TrustedQuery]] = None):
        self.trusted = trusted if trusted is not None else TRUSTED_QUERIES

    def route(self, question: str) -> RouteDecision:
        q = question.lower().strip()
        for tq in self.trusted:
            if any(self._matches(pat, q) for pat in tq.patterns):
                return RouteDecision(
                    route="trusted",
                    trusted=tq,
                    reason=f"matched trusted query '{tq.name}'",
                )
        return RouteDecision(route="genie", reason="no trusted match")

    @staticmethod
    def _matches(pattern: str, question: str) -> bool:
        return re.search(pattern, question, re.IGNORECASE) is not None
