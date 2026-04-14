"""Library of pre-approved parameterized SQL for high-frequency questions.

These bypass Genie (and the LLM-SQL-generation cost) entirely and run
directly on the configured SQL warehouse.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TrustedQuery:
    name: str
    description: str
    sql: str
    patterns: list[str] = field(default_factory=list)


TRUSTED_QUERIES: list[TrustedQuery] = [
    TrustedQuery(
        name="workspace_cost_last_30d",
        description="Total DBU spend over the last 30 days across all workspaces.",
        sql=(
            "SELECT SUM(usage_quantity) AS dbus "
            "FROM system.billing.usage "
            "WHERE usage_date >= current_date() - INTERVAL 30 DAYS"
        ),
        patterns=[
            r"total.*spend.*(last|past).*30",
            r"dbus?.*(last|past).*30",
            r"cost.*last.*month",
        ],
    ),
    TrustedQuery(
        name="top_skus_last_30d",
        description="Top 10 SKUs by spend in the last 30 days.",
        sql=(
            "SELECT sku_name, SUM(usage_quantity) AS dbus "
            "FROM system.billing.usage "
            "WHERE usage_date >= current_date() - INTERVAL 30 DAYS "
            "GROUP BY sku_name ORDER BY dbus DESC LIMIT 10"
        ),
        patterns=[
            r"top.*sku",
            r"which.*sku.*(cost|spend|expensive)",
        ],
    ),
]
