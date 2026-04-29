from __future__ import annotations

from typing import Any

from analytics.metrics.helper import build_output, safe_ratio


def build_metric(
    event_type_counts: dict[str, int],
    *,
    generated_at: str,
    total_event_count: int,
) -> dict[str, Any]:
    error_event_count = int(event_type_counts.get("error", 0))
    return build_output(
        generated_at=generated_at,
        total_event_count=total_event_count,
        error_event_count=error_event_count,
        error_event_ratio=safe_ratio(error_event_count, total_event_count),
    )
