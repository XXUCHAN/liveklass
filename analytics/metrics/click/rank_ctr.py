from __future__ import annotations

from typing import Any

from analytics.helpers import build_output, safe_ratio
from common.parsing import coerce_int


def build_metric(
    events: list[dict[str, Any]],
    *,
    generated_at: str,
    total_event_count: int,
) -> dict[str, Any]:
    impressions: dict[int, int] = {}
    clicks: dict[int, int] = {}

    for event in events:
        rank = coerce_int(event.get("rank"))
        if rank is None:
            continue

        event_type = event.get("event_type")
        if event_type == "impression":
            impressions[rank] = impressions.get(rank, 0) + 1
        elif event_type == "click":
            clicks[rank] = clicks.get(rank, 0) + 1

    rows = []
    for rank in sorted(set(impressions) | set(clicks)):
        impression_count = impressions.get(rank, 0)
        click_count = clicks.get(rank, 0)
        rows.append(
            {
                "rank": rank,
                "impressions": impression_count,
                "clicks": click_count,
                "ctr": safe_ratio(click_count, impression_count),
            }
        )

    return build_output(
        generated_at=generated_at,
        total_event_count=total_event_count,
        rows=rows,
    )
