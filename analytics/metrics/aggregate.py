from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from analytics.metrics.basic.error_event_ratio import build_metric as build_error_event_ratio
    from analytics.metrics.basic.event_type_counts import build_metric as build_event_type_counts
    from analytics.metrics.click.popularity_ctr import build_metric as build_popularity_ctr
    from analytics.metrics.click.presentation_ctr import build_metric as build_presentation_ctr
    from analytics.metrics.click.rank_ctr import build_metric as build_rank_ctr
    from analytics.metrics.helper import load_events
    from common.parsing import format_timestamp, utc_now
    from storage.opensearch_client import create_opensearch_service
except ModuleNotFoundError:
    import sys

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    from analytics.metrics.basic.error_event_ratio import build_metric as build_error_event_ratio  # type: ignore[no-redef]
    from analytics.metrics.basic.event_type_counts import build_metric as build_event_type_counts  # type: ignore[no-redef]
    from analytics.metrics.click.popularity_ctr import build_metric as build_popularity_ctr  # type: ignore[no-redef]
    from analytics.metrics.click.presentation_ctr import build_metric as build_presentation_ctr  # type: ignore[no-redef]
    from analytics.metrics.click.rank_ctr import build_metric as build_rank_ctr  # type: ignore[no-redef]
    from analytics.metrics.helper import load_events  # type: ignore[no-redef]
    from common.parsing import format_timestamp, utc_now  # type: ignore[no-redef]
    from storage.opensearch_client import create_opensearch_service  # type: ignore[no-redef]


CLICK_METRIC_SOURCE_FIELDS = [
    "event_type",
    "rank",
    "popularity_bucket",
    "presentation_type",
]


@dataclass(frozen=True)
class AnalyticsSettings:
    output_dir: Path
    wait_timeout_seconds: int
    wait_poll_seconds: float

    @classmethod
    def from_env(cls) -> "AnalyticsSettings":
        return cls(
            output_dir=Path(os.getenv("AGGREGATION_OUTPUT_DIR", "output/aggregations")),
            wait_timeout_seconds=int(os.getenv("ANALYTICS_WAIT_TIMEOUT_SECONDS", "60")),
            wait_poll_seconds=float(os.getenv("ANALYTICS_WAIT_POLL_SECONDS", "2")),
        )


def _wait_for_events(service: Any, settings: AnalyticsSettings) -> int:
    deadline = time.time() + settings.wait_timeout_seconds
    latest_count = 0

    while time.time() < deadline:
        response = service.client.count(index=service.settings.clickstream_index)
        latest_count = int(response.get("count", 0))
        if latest_count > 0:
            return latest_count

        print("Waiting for clickstream events to be indexed...")
        time.sleep(settings.wait_poll_seconds)

    return latest_count


def _clear_output_directory(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for output_file in output_dir.glob("*.json"):
        output_file.unlink()


def _write_json(output_path: Path, payload: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _build_runtime_context() -> tuple[Any, AnalyticsSettings, str, int]:
    settings = AnalyticsSettings.from_env()
    settings.output_dir.mkdir(parents=True, exist_ok=True)

    service = create_opensearch_service()
    service.wait_until_ready()
    service.ensure_indexes()

    total_event_count = _wait_for_events(service, settings)
    generated_at = format_timestamp(utc_now())
    return service, settings, generated_at, total_event_count


def main() -> None:
    service, settings, generated_at, total_event_count = _build_runtime_context()
    if total_event_count <= 0:
        print("No clickstream events found. Skipping analytics output.")
        return

    _clear_output_directory(settings.output_dir)

    click_events = load_events(
        service,
        run_cutoff=generated_at,
        source_fields=CLICK_METRIC_SOURCE_FIELDS,
        event_types=["impression", "click"],
    )

    event_type_counts = build_event_type_counts(
        service,
        generated_at=generated_at,
        total_event_count=total_event_count,
    )
    error_event_ratio = build_error_event_ratio(
        event_type_counts["counts"],
        generated_at=generated_at,
        total_event_count=total_event_count,
    )
    rank_ctr = build_rank_ctr(
        click_events,
        generated_at=generated_at,
        total_event_count=total_event_count,
    )
    popularity_ctr = build_popularity_ctr(
        click_events,
        generated_at=generated_at,
        total_event_count=total_event_count,
    )
    presentation_ctr = build_presentation_ctr(
        click_events,
        generated_at=generated_at,
        total_event_count=total_event_count,
    )

    outputs = {
        "event_type_counts.json": event_type_counts,
        "error_event_ratio.json": error_event_ratio,
        "rank_ctr.json": rank_ctr,
        "popularity_ctr.json": popularity_ctr,
        "presentation_ctr.json": presentation_ctr,
    }

    for filename, payload in outputs.items():
        _write_json(settings.output_dir / filename, payload)

    print(
        json.dumps(
            {
                "status": "completed",
                "generated_at": generated_at,
                "total_event_count": total_event_count,
                "loaded_click_events": len(click_events),
                "output_files": sorted(outputs),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
