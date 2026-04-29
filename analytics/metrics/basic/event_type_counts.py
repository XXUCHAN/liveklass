from __future__ import annotations

from typing import Any

from analytics.metrics.helper import build_output, search_aggregation


def build_metric(
    service: Any,
    *,
    generated_at: str,
    total_event_count: int,
) -> dict[str, Any]:
    aggregations = search_aggregation(
        service,
        run_cutoff=generated_at,
        aggs={
            "event_types": {
                "terms": {
                    "field": "event_type",
                    "size": 10,
                    "order": {"_key": "asc"},
                }
            }
        },
    )
    buckets = aggregations.get("event_types", {}).get("buckets", [])
    counts = {bucket["key"]: int(bucket["doc_count"]) for bucket in buckets}
    return build_output(
        generated_at=generated_at,
        total_event_count=total_event_count,
        counts=counts,
    )
