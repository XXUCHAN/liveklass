from __future__ import annotations

from typing import Any


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 6)


def build_output(
    *,
    generated_at: str,
    total_event_count: int,
    **fields: Any,
) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "total_event_count": total_event_count,
        **fields,
    }


def build_group_ctr_rows(
    events: list[dict[str, Any]],
    *,
    field_name: str,
) -> list[dict[str, Any]]:
    impressions: dict[str, int] = {}
    clicks: dict[str, int] = {}

    for event in events:
        event_type = event.get("event_type")
        group = event.get(field_name)
        if not isinstance(group, str) or not group:
            continue

        if event_type == "impression":
            impressions[group] = impressions.get(group, 0) + 1
        elif event_type == "click":
            clicks[group] = clicks.get(group, 0) + 1

    rows = []
    for group in sorted(set(impressions) | set(clicks)):
        impression_count = impressions.get(group, 0)
        click_count = clicks.get(group, 0)
        rows.append(
            {
                "group": group,
                "impressions": impression_count,
                "clicks": click_count,
                "ctr": safe_ratio(click_count, impression_count),
            }
        )
    return rows

