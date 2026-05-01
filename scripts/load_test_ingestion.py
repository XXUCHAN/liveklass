from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from generator.main import ClickstreamGenerator, GeneratorSettings


def _derive_health_url(api_url: str) -> str:
    if api_url.endswith("/events"):
        return f"{api_url[:-7]}/health"
    return f"{api_url.rstrip('/')}/health"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@dataclass(frozen=True)
class LoadTestSettings:
    api_url: str
    health_url: str
    duration_seconds: int
    concurrency: int
    total_sessions: int
    target_requests_per_second: float | None
    health_poll_interval_seconds: float
    request_timeout_seconds: float
    seed_base: int


@dataclass
class SharedStats:
    request_count: int = 0
    request_ok: int = 0
    request_failed: int = 0
    accepted_events: int = 0
    invalid_events: int = 0
    dead_lettered_events: int = 0
    latency_seconds: list[float] = field(default_factory=list)
    status_codes: Counter[int] = field(default_factory=Counter)
    exceptions: Counter[str] = field(default_factory=Counter)
    queue_sizes: list[int] = field(default_factory=list)
    health_statuses: Counter[str] = field(default_factory=Counter)
    payload_events: int = 0
    payload_bytes: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add_result(
        self,
        *,
        status_code: int,
        latency_seconds: float,
        accepted: int,
        invalid: int,
        dead_lettered: int,
        payload_events: int,
        payload_bytes: int,
    ) -> None:
        with self.lock:
            self.request_count += 1
            self.status_codes[status_code] += 1
            self.latency_seconds.append(latency_seconds)
            self.payload_events += payload_events
            self.payload_bytes += payload_bytes
            if 200 <= status_code < 300:
                self.request_ok += 1
                self.accepted_events += accepted
                self.invalid_events += invalid
                self.dead_lettered_events += dead_lettered
            else:
                self.request_failed += 1

    def add_exception(self, exception_name: str) -> None:
        with self.lock:
            self.request_count += 1
            self.request_failed += 1
            self.exceptions[exception_name] += 1

    def add_health(self, status: str, queue_size: int | None) -> None:
        with self.lock:
            self.health_statuses[status] += 1
            if queue_size is not None:
                self.queue_sizes.append(queue_size)


def _make_generator_settings(total_sessions: int, seed: int) -> GeneratorSettings:
    return GeneratorSettings(
        api_url="http://127.0.0.1:8000/events",
        send_to_api=False,
        mode="dev",
        total_sessions=total_sessions,
        batch_size=100,
        catalog_size=60,
        max_results_per_page=10,
        max_clicks_per_session=2,
        purchase_rate=0.22,
        error_rate=0.05,
        invalid_event_probability=0.0,
        request_timeout_seconds=10,
        health_retries=1,
        retry_delay_seconds=1,
        cycle_interval_seconds=10,
        max_cycles=1,
        seed=seed,
        ensure_all_event_types=True,
    )


def _build_payload(total_sessions: int, seed: int) -> list[dict[str, Any]]:
    generator = ClickstreamGenerator(_make_generator_settings(total_sessions, seed))
    return generator.generate()


def _post_events(
    api_url: str,
    payload: list[dict[str, Any]],
    timeout_seconds: float,
) -> tuple[int, dict[str, Any], int]:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        api_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        response_body = response.read()
        response_json = json.loads(response_body.decode("utf-8"))
        return response.status, response_json, len(body)


def _read_health(health_url: str, timeout_seconds: float) -> dict[str, Any]:
    with urllib.request.urlopen(health_url, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _worker(
    worker_id: int,
    settings: LoadTestSettings,
    shared_stats: SharedStats,
    stop_at: float,
) -> None:
    seed = settings.seed_base + (worker_id * 1_000_000)
    per_worker_target_rps = (
        settings.target_requests_per_second / settings.concurrency
        if settings.target_requests_per_second is not None
        else None
    )
    next_due_at = time.monotonic()

    while time.monotonic() < stop_at:
        if per_worker_target_rps and per_worker_target_rps > 0:
            now = time.monotonic()
            if now < next_due_at:
                time.sleep(next_due_at - now)
            next_due_at += 1.0 / per_worker_target_rps

        payload = _build_payload(settings.total_sessions, seed)
        seed += 1
        started_at = time.monotonic()
        try:
            status_code, response_json, payload_bytes = _post_events(
                settings.api_url,
                payload,
                settings.request_timeout_seconds,
            )
            latency_seconds = time.monotonic() - started_at
            shared_stats.add_result(
                status_code=status_code,
                latency_seconds=latency_seconds,
                accepted=int(response_json.get("accepted", 0)),
                invalid=int(response_json.get("invalid", 0)),
                dead_lettered=int(response_json.get("dead_lettered", 0)),
                payload_events=len(payload),
                payload_bytes=payload_bytes,
            )
        except urllib.error.HTTPError as exc:
            shared_stats.add_result(
                status_code=exc.code,
                latency_seconds=time.monotonic() - started_at,
                accepted=0,
                invalid=0,
                dead_lettered=0,
                payload_events=len(payload),
                payload_bytes=len(json.dumps(payload).encode("utf-8")),
            )
        except Exception as exc:  # noqa: BLE001
            shared_stats.add_exception(type(exc).__name__)


def _health_poller(settings: LoadTestSettings, shared_stats: SharedStats, stop_at: float) -> None:
    while time.monotonic() < stop_at:
        try:
            health = _read_health(settings.health_url, settings.request_timeout_seconds)
            shared_stats.add_health(
                status=str(health.get("status", "unknown")),
                queue_size=int(health.get("queue_size")) if "queue_size" in health else None,
            )
        except Exception as exc:  # noqa: BLE001
            shared_stats.add_health(status=f"error:{type(exc).__name__}", queue_size=None)

        time.sleep(settings.health_poll_interval_seconds)


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0

    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * percentile))
    return ordered[index]


def _build_summary(
    settings: LoadTestSettings,
    shared_stats: SharedStats,
    started_at: float,
    finished_at: float,
) -> dict[str, Any]:
    elapsed_seconds = max(0.001, finished_at - started_at)
    latencies = shared_stats.latency_seconds

    return {
        "started_at": _now_iso(),
        "duration_seconds": round(elapsed_seconds, 3),
        "concurrency": settings.concurrency,
        "total_sessions_per_request": settings.total_sessions,
        "target_requests_per_second": settings.target_requests_per_second,
        "request_count": shared_stats.request_count,
        "request_ok": shared_stats.request_ok,
        "request_failed": shared_stats.request_failed,
        "status_codes": dict(shared_stats.status_codes),
        "exceptions": dict(shared_stats.exceptions),
        "payload_events_total": shared_stats.payload_events,
        "payload_bytes_total": shared_stats.payload_bytes,
        "avg_events_per_request": round(
            shared_stats.payload_events / shared_stats.request_count, 3
        )
        if shared_stats.request_count
        else 0.0,
        "accepted_events": shared_stats.accepted_events,
        "invalid_events": shared_stats.invalid_events,
        "dead_lettered_events": shared_stats.dead_lettered_events,
        "requests_per_second": round(shared_stats.request_count / elapsed_seconds, 3),
        "accepted_events_per_second": round(shared_stats.accepted_events / elapsed_seconds, 3),
        "payload_megabytes_per_second": round(
            (shared_stats.payload_bytes / (1024 * 1024)) / elapsed_seconds, 3
        ),
        "latency_ms": {
            "avg": round(mean(latencies) * 1000, 2) if latencies else 0.0,
            "p50": round(_percentile(latencies, 0.50) * 1000, 2),
            "p95": round(_percentile(latencies, 0.95) * 1000, 2),
            "p99": round(_percentile(latencies, 0.99) * 1000, 2),
            "max": round(max(latencies) * 1000, 2) if latencies else 0.0,
        },
        "health": {
            "statuses": dict(shared_stats.health_statuses),
            "queue_size": {
                "avg": round(mean(shared_stats.queue_sizes), 2)
                if shared_stats.queue_sizes
                else 0.0,
                "max": max(shared_stats.queue_sizes) if shared_stats.queue_sizes else 0,
            },
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load test the ingestion API with generated clickstream payloads.",
    )
    parser.add_argument("--api-url", default="http://127.0.0.1:8000/events")
    parser.add_argument("--duration-seconds", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--total-sessions", type=int, default=7)
    parser.add_argument("--target-requests-per-second", type=float)
    parser.add_argument("--health-poll-interval-seconds", type=float, default=1.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--seed-base", type=int, default=10_000)
    args = parser.parse_args()

    settings = LoadTestSettings(
        api_url=args.api_url,
        health_url=_derive_health_url(args.api_url),
        duration_seconds=args.duration_seconds,
        concurrency=args.concurrency,
        total_sessions=args.total_sessions,
        target_requests_per_second=args.target_requests_per_second,
        health_poll_interval_seconds=args.health_poll_interval_seconds,
        request_timeout_seconds=args.request_timeout_seconds,
        seed_base=args.seed_base,
    )

    shared_stats = SharedStats()
    started_at = time.monotonic()
    stop_at = started_at + settings.duration_seconds

    health_thread = threading.Thread(
        target=_health_poller,
        args=(settings, shared_stats, stop_at),
        daemon=True,
        name="health-poller",
    )
    health_thread.start()

    workers = [
        threading.Thread(
            target=_worker,
            args=(worker_id, settings, shared_stats, stop_at),
            daemon=True,
            name=f"worker-{worker_id}",
        )
        for worker_id in range(settings.concurrency)
    ]

    for worker in workers:
        worker.start()

    for worker in workers:
        worker.join()
    health_thread.join(timeout=2.0)

    finished_at = time.monotonic()
    print(json.dumps(_build_summary(settings, shared_stats, started_at, finished_at), indent=2))


if __name__ == "__main__":
    main()
