from __future__ import annotations

import json
import os
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from opensearchpy.helpers import scan

try:
    from ingestion.opensearch_client import create_opensearch_service
    from ingestion.validation import ALLOWED_EVENT_TYPES
except ModuleNotFoundError:
    import sys

    PROJECT_ROOT = Path(__file__).resolve().parent.parent
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    from ingestion.opensearch_client import create_opensearch_service  # type: ignore[no-redef]
    from ingestion.validation import ALLOWED_EVENT_TYPES  # type: ignore[no-redef]


COMMON_REQUIRED_FIELDS = (
    "event_id",
    "schema_version",
    "event_type",
    "user_id",
    "session_id",
    "event_time",
    "device",
)

REQUIRED_FIELDS_BY_TYPE: dict[str, tuple[str, ...]] = {
    "page_view": ("page_url",),
    "impression": (
        "query",
        "item_id",
        "rank",
        "popularity_bucket",
        "presentation_type",
    ),
    "click": (
        "query",
        "item_id",
        "rank",
        "click_prob",
        "popularity_bucket",
        "presentation_type",
    ),
    "purchase": ("item_id", "amount"),
    "error": ("error_code", "error_message"),
}

SOURCE_FIELDS = [
    "event_id",
    "schema_version",
    "event_type",
    "user_id",
    "session_id",
    "event_time",
    "device",
    "page_url",
    "query",
    "item_id",
    "rank",
    "popularity_bucket",
    "presentation_type",
    "position_bias",
    "click_prob",
    "amount",
    "error_code",
    "error_message",
    "received_at",
    "ingested_at",
    "arrival_lag_seconds",
    "is_late_arrival",
]

QUALITY_JOB_NAME = os.getenv("QUALITY_JOB_NAME", "incremental-clickstream-quality")
QUALITY_MODE = os.getenv("QUALITY_MODE", "dev").strip().lower()
QUALITY_OVERLAP_WINDOW_SECONDS = int(os.getenv("QUALITY_OVERLAP_WINDOW_SECONDS", "600"))
QUALITY_WAIT_TIMEOUT_SECONDS = int(os.getenv("QUALITY_WAIT_TIMEOUT_SECONDS", "60"))
QUALITY_WAIT_POLL_SECONDS = float(os.getenv("QUALITY_WAIT_POLL_SECONDS", "2"))
QUALITY_INTERVAL_SECONDS = float(os.getenv("QUALITY_INTERVAL_SECONDS", "15"))
QUALITY_MAX_CYCLES = int(os.getenv("QUALITY_MAX_CYCLES", "0"))
MAX_LATE_ARRIVAL_RATE = float(os.getenv("QUALITY_MAX_LATE_ARRIVAL_RATE", "0.10"))


@dataclass(frozen=True)
class QualityCheckResult:
    check_name: str
    status: str
    failed_count: int
    checked_at: str
    details: dict[str, Any]


@dataclass(frozen=True)
class QualityEventWindow:
    checkpoint_ingested_at: str | None
    window_start_ingested_at: str | None
    run_cutoff: str
    incremental_events: list[dict[str, Any]]
    window_events: list[dict[str, Any]]


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_timestamp() -> str:
    return _format_timestamp(_utc_now())


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None

    candidate = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _event_checkpoint_time(event: dict[str, Any]) -> datetime | None:
    for field_name in ("ingested_at", "received_at", "event_time"):
        parsed = _parse_timestamp(event.get(field_name))
        if parsed is not None:
            return parsed
    return None


def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _build_result(
    check_name: str,
    *,
    failed_count: int,
    details: dict[str, Any],
    checked_at: str,
    status: str | None = None,
) -> QualityCheckResult:
    return QualityCheckResult(
        check_name=check_name,
        status=status or ("passed" if failed_count == 0 else "failed"),
        failed_count=failed_count,
        checked_at=checked_at,
        details=details,
    )


def _wait_for_initial_events(service: Any, *, checkpoint_exists: bool) -> int:
    latest_count = 0
    if checkpoint_exists:
        response = service.client.count(index=service.settings.clickstream_index)
        return int(response.get("count", 0))

    deadline = time.time() + QUALITY_WAIT_TIMEOUT_SECONDS
    while time.time() < deadline:
        response = service.client.count(index=service.settings.clickstream_index)
        latest_count = int(response.get("count", 0))
        if latest_count > 0:
            return latest_count

        print("Waiting for clickstream events to be indexed...")
        time.sleep(QUALITY_WAIT_POLL_SECONDS)

    return latest_count


def _load_events(
    service: Any,
    *,
    lower_bound: str | None = None,
    upper_bound: str | None = None,
) -> list[dict[str, Any]]:
    if lower_bound is None and upper_bound is None:
        query: dict[str, Any] = {"query": {"match_all": {}}}
    else:
        range_filter: dict[str, Any] = {}
        if lower_bound is not None:
            range_filter["gte"] = lower_bound
        if upper_bound is not None:
            range_filter["lte"] = upper_bound
        query = {"query": {"range": {"ingested_at": range_filter}}}

    documents: list[dict[str, Any]] = []
    for hit in scan(
        service.client,
        index=service.settings.clickstream_index,
        query=query,
        _source=SOURCE_FIELDS,
        size=500,
        preserve_order=False,
        request_timeout=service.settings.opensearch_timeout_seconds,
    ):
        source = hit.get("_source")
        if isinstance(source, dict):
            documents.append(source)
    return documents


def _load_incremental_event_window(
    service: Any,
    *,
    checkpoint_ingested_at: str | None,
    run_cutoff: str,
) -> QualityEventWindow:
    if checkpoint_ingested_at is None:
        window_events = _load_events(service)
        return QualityEventWindow(
            checkpoint_ingested_at=None,
            window_start_ingested_at=None,
            run_cutoff=run_cutoff,
            incremental_events=list(window_events),
            window_events=window_events,
        )

    checkpoint_dt = _parse_timestamp(checkpoint_ingested_at)
    if checkpoint_dt is None:
        raise RuntimeError(f"invalid checkpoint timestamp: {checkpoint_ingested_at}")

    window_start_dt = checkpoint_dt - timedelta(seconds=QUALITY_OVERLAP_WINDOW_SECONDS)
    window_start = _format_timestamp(window_start_dt)
    window_events = _load_events(service, lower_bound=window_start, upper_bound=run_cutoff)

    incremental_events: list[dict[str, Any]] = []
    for event in window_events:
        event_time = _event_checkpoint_time(event)
        if event_time is not None and event_time > checkpoint_dt:
            incremental_events.append(event)

    return QualityEventWindow(
        checkpoint_ingested_at=checkpoint_ingested_at,
        window_start_ingested_at=window_start,
        run_cutoff=run_cutoff,
        incremental_events=incremental_events,
        window_events=window_events,
    )


def _max_checkpoint_timestamp(events: list[dict[str, Any]]) -> str | None:
    parsed_times = [parsed for event in events if (parsed := _event_checkpoint_time(event)) is not None]
    if not parsed_times:
        return None
    return _format_timestamp(max(parsed_times))


def _required_field_null_check(
    incremental_events: list[dict[str, Any]], checked_at: str
) -> QualityCheckResult:
    field_missing_counts: Counter[str] = Counter()
    sample_failures: list[dict[str, Any]] = []
    failed_records = 0

    for event in incremental_events:
        event_type = event.get("event_type")
        required_fields = list(COMMON_REQUIRED_FIELDS)
        if isinstance(event_type, str):
            required_fields.extend(REQUIRED_FIELDS_BY_TYPE.get(event_type, ()))

        missing_fields = [field_name for field_name in required_fields if _is_missing(event.get(field_name))]
        if not missing_fields:
            continue

        failed_records += 1
        field_missing_counts.update(missing_fields)
        if len(sample_failures) < 5:
            sample_failures.append(
                {
                    "event_id": event.get("event_id", "missing-event-id"),
                    "event_type": event_type,
                    "missing_fields": missing_fields,
                }
            )

    return _build_result(
        "required_field_null_check",
        failed_count=failed_records,
        details={
            "missing_field_counts": dict(field_missing_counts),
            "sample_failures": sample_failures,
        },
        checked_at=checked_at,
    )


def _invalid_event_type_check(
    incremental_events: list[dict[str, Any]], checked_at: str
) -> QualityCheckResult:
    invalid_types: Counter[str] = Counter()

    for event in incremental_events:
        event_type = event.get("event_type")
        if not isinstance(event_type, str) or event_type not in ALLOWED_EVENT_TYPES:
            invalid_types[str(event_type)] += 1

    return _build_result(
        "invalid_event_type_check",
        failed_count=sum(invalid_types.values()),
        details={"invalid_event_types": dict(invalid_types)},
        checked_at=checked_at,
    )


def _duplicate_event_id_check(
    window_events: list[dict[str, Any]],
    incremental_events: list[dict[str, Any]],
    checked_at: str,
) -> QualityCheckResult:
    incremental_event_ids = {
        event_id
        for event in incremental_events
        if isinstance((event_id := event.get("event_id")), str) and event_id.strip()
    }

    event_id_counts: Counter[str] = Counter()
    for event in window_events:
        event_id = event.get("event_id")
        if isinstance(event_id, str) and event_id.strip():
            event_id_counts[event_id] += 1

    duplicates = {
        event_id: count
        for event_id, count in event_id_counts.items()
        if count > 1 and event_id in incremental_event_ids
    }
    duplicate_rows = sum(count - 1 for count in duplicates.values())
    top_duplicates = dict(sorted(duplicates.items(), key=lambda item: item[1], reverse=True)[:10])

    return _build_result(
        "duplicate_event_id_check",
        failed_count=duplicate_rows,
        details={
            "duplicate_event_ids": top_duplicates,
            "duplicate_key_count": len(duplicates),
        },
        checked_at=checked_at,
    )


def _rank_range_check(
    incremental_events: list[dict[str, Any]], checked_at: str
) -> QualityCheckResult:
    failed_records = 0
    samples: list[dict[str, Any]] = []

    for event in incremental_events:
        rank = event.get("rank")
        if rank is None:
            continue

        rank_value = _coerce_int(rank)
        if rank_value is not None and 1 <= rank_value <= 10:
            continue

        failed_records += 1
        if len(samples) < 5:
            samples.append(
                {
                    "event_id": event.get("event_id", "missing-event-id"),
                    "event_type": event.get("event_type"),
                    "rank": rank,
                }
            )

    return _build_result(
        "rank_range_check",
        failed_count=failed_records,
        details={"sample_failures": samples},
        checked_at=checked_at,
    )


def _click_prob_range_check(
    incremental_events: list[dict[str, Any]], checked_at: str
) -> QualityCheckResult:
    failed_records = 0
    samples: list[dict[str, Any]] = []

    for event in incremental_events:
        click_prob = event.get("click_prob")
        if click_prob is None:
            continue

        click_prob_value = _coerce_float(click_prob)
        if click_prob_value is not None and 0.0 <= click_prob_value <= 1.0:
            continue

        failed_records += 1
        if len(samples) < 5:
            samples.append(
                {
                    "event_id": event.get("event_id", "missing-event-id"),
                    "event_type": event.get("event_type"),
                    "click_prob": click_prob,
                }
            )

    return _build_result(
        "click_prob_range_check",
        failed_count=failed_records,
        details={"sample_failures": samples},
        checked_at=checked_at,
    )


def _position_bias_range_check(
    incremental_events: list[dict[str, Any]], checked_at: str
) -> QualityCheckResult:
    failed_records = 0
    samples: list[dict[str, Any]] = []

    for event in incremental_events:
        position_bias = event.get("position_bias")
        if position_bias is None:
            continue

        position_bias_value = _coerce_float(position_bias)
        if position_bias_value is not None and 0.0 <= position_bias_value <= 1.0:
            continue

        failed_records += 1
        if len(samples) < 5:
            samples.append(
                {
                    "event_id": event.get("event_id", "missing-event-id"),
                    "event_type": event.get("event_type"),
                    "position_bias": position_bias,
                }
            )

    return _build_result(
        "position_bias_range_check",
        failed_count=failed_records,
        details={"sample_failures": samples},
        checked_at=checked_at,
    )


def _click_impression_consistency_check(
    window_events: list[dict[str, Any]],
    incremental_events: list[dict[str, Any]],
    checked_at: str,
) -> QualityCheckResult:
    touched_keys: set[tuple[str, str, str]] = set()
    for event in incremental_events:
        event_type = event.get("event_type")
        if event_type not in {"impression", "click"}:
            continue

        item_id = event.get("item_id")
        session_id = event.get("session_id")
        query = event.get("query")
        if _is_missing(item_id) or _is_missing(session_id) or _is_missing(query):
            continue

        touched_keys.add((str(item_id), str(session_id), str(query)))

    impression_counts: defaultdict[tuple[str, str, str], int] = defaultdict(int)
    click_counts: defaultdict[tuple[str, str, str], int] = defaultdict(int)

    for event in window_events:
        event_type = event.get("event_type")
        if event_type not in {"impression", "click"}:
            continue

        item_id = event.get("item_id")
        session_id = event.get("session_id")
        query = event.get("query")
        if _is_missing(item_id) or _is_missing(session_id) or _is_missing(query):
            continue

        key = (str(item_id), str(session_id), str(query))
        if event_type == "impression":
            impression_counts[key] += 1
        else:
            click_counts[key] += 1

    offenders: list[dict[str, Any]] = []
    for key in touched_keys:
        click_count = click_counts.get(key, 0)
        impression_count = impression_counts.get(key, 0)
        if click_count <= impression_count:
            continue

        item_id, session_id, query = key
        offenders.append(
            {
                "item_id": item_id,
                "session_id": session_id,
                "query": query,
                "impression_count": impression_count,
                "click_count": click_count,
            }
        )

    offenders.sort(key=lambda item: item["click_count"] - item["impression_count"], reverse=True)
    return _build_result(
        "click_impression_consistency_check",
        failed_count=len(offenders),
        details={"sample_failures": offenders[:10]},
        checked_at=checked_at,
    )


def _late_arrival_event_check(
    incremental_events: list[dict[str, Any]], checked_at: str
) -> QualityCheckResult:
    late_events: list[dict[str, Any]] = []
    by_event_type: Counter[str] = Counter()
    max_lag_seconds = 0.0

    for event in incremental_events:
        if event.get("is_late_arrival") is not True:
            continue

        late_events.append(event)
        by_event_type[str(event.get("event_type"))] += 1

        arrival_lag_seconds = _coerce_float(event.get("arrival_lag_seconds")) or 0.0
        max_lag_seconds = max(max_lag_seconds, arrival_lag_seconds)

    late_event_count = len(late_events)
    late_arrival_rate = late_event_count / len(incremental_events) if incremental_events else 0.0
    allowed_late_events = int(len(incremental_events) * MAX_LATE_ARRIVAL_RATE)
    sample_failures = [
        {
            "event_id": event.get("event_id", "missing-event-id"),
            "event_type": event.get("event_type"),
            "event_time": event.get("event_time"),
            "ingested_at": event.get("ingested_at"),
            "arrival_lag_seconds": event.get("arrival_lag_seconds"),
        }
        for event in late_events[:5]
    ]

    status = "passed" if late_event_count <= allowed_late_events else "failed"
    return _build_result(
        "late_arrival_event_check",
        failed_count=late_event_count,
        details={
            "late_arrival_rate": round(late_arrival_rate, 4),
            "allowed_late_arrival_rate": MAX_LATE_ARRIVAL_RATE,
            "late_event_count_by_type": dict(by_event_type),
            "max_arrival_lag_seconds": max_lag_seconds,
            "sample_failures": sample_failures,
        },
        checked_at=checked_at,
        status=status,
    )


def _checkpoint_document(
    *,
    previous_checkpoint: str | None,
    next_checkpoint: str | None,
    checked_at: str,
    run_cutoff: str,
    incremental_count: int,
    window_count: int,
) -> dict[str, Any]:
    return {
        "last_checked_ingested_at": next_checkpoint or previous_checkpoint,
        "last_checked_at": checked_at,
        "last_run_cutoff": run_cutoff,
        "overlap_window_seconds": QUALITY_OVERLAP_WINDOW_SECONDS,
        "last_incremental_count": incremental_count,
        "last_window_count": window_count,
    }


def run_quality_checks() -> dict[str, Any]:
    service = create_opensearch_service()
    service.wait_until_ready()
    service.ensure_indexes()

    checkpoint = service.get_quality_checkpoint(QUALITY_JOB_NAME)
    checkpoint_ingested_at = None
    if isinstance(checkpoint, dict):
        checkpoint_value = checkpoint.get("last_checked_ingested_at")
        if isinstance(checkpoint_value, str) and checkpoint_value.strip():
            checkpoint_ingested_at = checkpoint_value

    observed_count = _wait_for_initial_events(
        service,
        checkpoint_exists=checkpoint_ingested_at is not None,
    )
    checked_at = _utc_timestamp()
    event_window = _load_incremental_event_window(
        service,
        checkpoint_ingested_at=checkpoint_ingested_at,
        run_cutoff=checked_at,
    )

    if checkpoint_ingested_at is not None and not event_window.incremental_events:
        service.upsert_quality_checkpoint(
            QUALITY_JOB_NAME,
            _checkpoint_document(
                previous_checkpoint=checkpoint_ingested_at,
                next_checkpoint=None,
                checked_at=checked_at,
                run_cutoff=event_window.run_cutoff,
                incremental_count=0,
                window_count=len(event_window.window_events),
            ),
        )
        return {
            "job_name": QUALITY_JOB_NAME,
            "checked_at": checked_at,
            "status": "no_new_events",
            "observed_event_count": observed_count,
            "incremental_event_count": 0,
            "window_event_count": len(event_window.window_events),
            "checkpoint_ingested_at": checkpoint_ingested_at,
            "next_checkpoint_ingested_at": checkpoint_ingested_at,
            "results": [],
        }

    results = [
        _required_field_null_check(event_window.incremental_events, checked_at),
        _invalid_event_type_check(event_window.incremental_events, checked_at),
        _duplicate_event_id_check(
            event_window.window_events,
            event_window.incremental_events,
            checked_at,
        ),
        _rank_range_check(event_window.incremental_events, checked_at),
        _click_prob_range_check(event_window.incremental_events, checked_at),
        _position_bias_range_check(event_window.incremental_events, checked_at),
        _click_impression_consistency_check(
            event_window.window_events,
            event_window.incremental_events,
            checked_at,
        ),
        _late_arrival_event_check(event_window.incremental_events, checked_at),
    ]

    next_checkpoint = _max_checkpoint_timestamp(event_window.incremental_events)
    run_metadata = {
        "job_name": QUALITY_JOB_NAME,
        "checkpoint_ingested_at": checkpoint_ingested_at,
        "window_start_ingested_at": event_window.window_start_ingested_at,
        "run_cutoff": event_window.run_cutoff,
        "incremental_event_count": len(event_window.incremental_events),
        "window_event_count": len(event_window.window_events),
    }

    for result in results:
        document = asdict(result)
        document["details"] = {**run_metadata, **document["details"]}
        service.index_quality_result(document)

    service.upsert_quality_checkpoint(
        QUALITY_JOB_NAME,
        _checkpoint_document(
            previous_checkpoint=checkpoint_ingested_at,
            next_checkpoint=next_checkpoint,
            checked_at=checked_at,
            run_cutoff=event_window.run_cutoff,
            incremental_count=len(event_window.incremental_events),
            window_count=len(event_window.window_events),
        ),
    )

    return {
        "job_name": QUALITY_JOB_NAME,
        "checked_at": checked_at,
        "status": "completed",
        "observed_event_count": observed_count,
        "incremental_event_count": len(event_window.incremental_events),
        "window_event_count": len(event_window.window_events),
        "checkpoint_ingested_at": checkpoint_ingested_at,
        "next_checkpoint_ingested_at": next_checkpoint or checkpoint_ingested_at,
        "results": [asdict(result) for result in results],
    }


def run_oneshot() -> None:
    summary = run_quality_checks()
    print(json.dumps({**summary, "mode": QUALITY_MODE, "execution_pattern": "oneshot"}, indent=2))


def run_periodic() -> None:
    cycle = 0
    while True:
        cycle += 1
        summary = run_quality_checks()
        print(
            json.dumps(
                {
                    **summary,
                    "mode": QUALITY_MODE,
                    "execution_pattern": "periodic",
                    "cycle": cycle,
                    "sleep_seconds": QUALITY_INTERVAL_SECONDS,
                },
                indent=2,
            )
        )

        if QUALITY_MAX_CYCLES > 0 and cycle >= QUALITY_MAX_CYCLES:
            return

        time.sleep(QUALITY_INTERVAL_SECONDS)


def main() -> None:
    if QUALITY_MODE == "prod":
        run_periodic()
        return

    run_oneshot()


if __name__ == "__main__":
    main()
