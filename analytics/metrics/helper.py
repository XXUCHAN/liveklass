from __future__ import annotations

from typing import Any

from opensearchpy.helpers import scan


def build_filtered_query(
    *,
    run_cutoff: str | None = None,
    event_types: list[str] | None = None,
) -> dict[str, Any]:
    filters: list[dict[str, Any]] = []

    if run_cutoff:
        filters.append({"range": {"ingested_at": {"lte": run_cutoff}}})

    if event_types:
        filters.append({"terms": {"event_type": event_types}})

    if not filters:
        return {"match_all": {}}

    return {"bool": {"filter": filters}}


def search_aggregation(
    service: Any,
    *,
    aggs: dict[str, Any],
    run_cutoff: str | None = None,
    event_types: list[str] | None = None,
) -> dict[str, Any]:
    response = service.client.search(
        index=service.settings.clickstream_index,
        body={
            "size": 0,
            "query": build_filtered_query(
                run_cutoff=run_cutoff,
                event_types=event_types,
            ),
            "aggs": aggs,
        },
        request_timeout=service.settings.opensearch_timeout_seconds,
    )
    return response.get("aggregations", {})


def load_events(
    service: Any,
    *,
    run_cutoff: str | None = None,
    source_fields: list[str] | None = None,
    event_types: list[str] | None = None,
) -> list[dict[str, Any]]:
    query = {
        "query": build_filtered_query(
            run_cutoff=run_cutoff,
            event_types=event_types,
        )
    }

    documents: list[dict[str, Any]] = []
    for hit in scan(
        service.client,
        index=service.settings.clickstream_index,
        query=query,
        _source=source_fields,
        size=500,
        preserve_order=False,
        request_timeout=service.settings.opensearch_timeout_seconds,
    ):
        source = hit.get("_source")
        if isinstance(source, dict):
            documents.append(source)
    return documents


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
