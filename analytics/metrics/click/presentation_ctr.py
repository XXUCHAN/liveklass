from __future__ import annotations

from typing import Any

from analytics.metrics.helper import (
    ImpressionSample,
    LogisticClickModel,
    build_group_ctr_rows,
    build_output,
)


def build_metric(
    events: list[ImpressionSample],
    *,
    generated_at: str,
    total_event_count: int,
    model: LogisticClickModel,
) -> dict[str, Any]:
    return build_output(
        generated_at=generated_at,
        total_event_count=total_event_count,
        rows=build_group_ctr_rows(events, field_name="presentation_type", model=model),
    )
