from __future__ import annotations

from typing import Any

from analytics.metrics.helper import (
    ImpressionSample,
    LogisticClickModel,
    build_output,
    build_rank_ctr_rows,
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
        rows=build_rank_ctr_rows(events, model=model),
    )
