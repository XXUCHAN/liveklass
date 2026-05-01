from __future__ import annotations

import argparse
import copy
import json
import sys
import threading
import time
import types
from collections import deque
from dataclasses import dataclass, field, replace
from pathlib import Path
from statistics import mean
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Local queue benchmarking does not need a real OpenSearch client.
if "opensearchpy" not in sys.modules:
    opensearch_module = types.ModuleType("opensearchpy")
    opensearch_module.OpenSearch = object
    opensearch_module.helpers = types.SimpleNamespace()
    sys.modules["opensearchpy"] = opensearch_module
    exceptions_module = types.ModuleType("opensearchpy.exceptions")
    exceptions_module.NotFoundError = RuntimeError
    sys.modules["opensearchpy.exceptions"] = exceptions_module

from generator.main import ClickstreamGenerator, GeneratorSettings
from ingestion.queue_worker import EventQueueWorker, QueueFullError
from ingestion.settings import settings as base_ingestion_settings


def _deep_size(value: Any, seen: set[int] | None = None) -> int:
    if seen is None:
        seen = set()

    object_id = id(value)
    if object_id in seen:
        return 0
    seen.add(object_id)

    size = sys.getsizeof(value)
    if isinstance(value, dict):
        return size + sum(_deep_size(key, seen) + _deep_size(item, seen) for key, item in value.items())
    if isinstance(value, (list, tuple, set, frozenset, deque)):
        return size + sum(_deep_size(item, seen) for item in value)
    return size


class FakeOpenSearchService:
    def __init__(self, bulk_latency_seconds: float) -> None:
        self.bulk_latency_seconds = bulk_latency_seconds
        self.bulk_calls = 0
        self.bulk_events = 0
        self.dead_letter_calls = 0
        self.dead_letter_events = 0
        self.lock = threading.Lock()

    def bulk_index_events(self, batch: list[dict[str, Any]]) -> int:
        if self.bulk_latency_seconds > 0:
            time.sleep(self.bulk_latency_seconds)
        with self.lock:
            self.bulk_calls += 1
            self.bulk_events += len(batch)
        return len(batch)

    def bulk_index_dead_letters(self, docs: list[dict[str, Any]]) -> int:
        with self.lock:
            self.dead_letter_calls += 1
            self.dead_letter_events += len(docs)
        return len(docs)


@dataclass(frozen=True)
class QueueBenchSettings:
    total_sessions_per_request: int
    enqueue_concurrency: int
    sustained_duration_seconds: int
    target_events_per_second: int
    bulk_latency_ms: float
    queue_batch_size: int
    queue_flush_interval_seconds: float
    queue_max_size: int
    prefill_events_for_drain: int
    generator_seed: int


@dataclass
class SustainedStats:
    sent_requests: int = 0
    accepted_requests: int = 0
    rejected_requests: int = 0
    sent_events: int = 0
    accepted_events: int = 0
    rejected_events: int = 0
    queue_sizes: list[int] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add_queue_size(self, queue_size: int) -> None:
        with self.lock:
            self.queue_sizes.append(queue_size)

    def add_request(self, event_count: int, accepted: bool) -> None:
        with self.lock:
            self.sent_requests += 1
            self.sent_events += event_count
            if accepted:
                self.accepted_requests += 1
                self.accepted_events += event_count
            else:
                self.rejected_requests += 1
                self.rejected_events += event_count


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


def _build_request_payload(total_sessions: int, seed: int) -> list[dict[str, Any]]:
    generator = ClickstreamGenerator(_make_generator_settings(total_sessions, seed))
    return generator.generate()


def _sample_payload_shape(total_sessions: int, seed: int) -> dict[str, Any]:
    payload = _build_request_payload(total_sessions, seed)
    event_json_sizes = [len(json.dumps(event, separators=(",", ":")).encode("utf-8")) for event in payload]
    event_heap_sizes = [_deep_size(event) for event in payload]
    avg_event_heap_bytes = mean(event_heap_sizes) if event_heap_sizes else 0.0
    return {
        "events_per_request": len(payload),
        "avg_event_json_bytes": round(mean(event_json_sizes), 2) if event_json_sizes else 0.0,
        "avg_event_heap_bytes": round(avg_event_heap_bytes, 2),
        "sample_request_heap_bytes": _deep_size(payload),
    }


def _make_queue_worker(settings: QueueBenchSettings, sink: FakeOpenSearchService) -> EventQueueWorker:
    ingestion_settings = replace(
        base_ingestion_settings,
        batch_size=settings.queue_batch_size,
        flush_interval_seconds=settings.queue_flush_interval_seconds,
        max_queue_size=settings.queue_max_size,
    )
    return EventQueueWorker(sink, ingestion_settings)


def _run_burst_fill(settings: QueueBenchSettings, events_per_request: int) -> dict[str, Any]:
    sink = FakeOpenSearchService(bulk_latency_seconds=9999.0)
    worker = _make_queue_worker(settings, sink)

    started_at = time.monotonic()
    seed = settings.generator_seed
    accepted_requests = 0
    accepted_events = 0

    while True:
        payload = _build_request_payload(settings.total_sessions_per_request, seed)
        seed += 1
        try:
            worker.enqueue_many(payload)
            accepted_requests += 1
            accepted_events += len(payload)
        except QueueFullError:
            break

    elapsed_seconds = max(0.001, time.monotonic() - started_at)
    queue_size = worker.queue_size()

    return {
        "accepted_requests_before_full": accepted_requests,
        "accepted_events_before_full": accepted_events,
        "time_to_full_seconds": round(elapsed_seconds, 4),
        "enqueue_events_per_second": round(accepted_events / elapsed_seconds, 2),
        "final_queue_size": queue_size,
        "configured_max_queue_size": settings.queue_max_size,
        "estimated_burst_payloads": round(queue_size / max(1, events_per_request), 2),
    }


def _queue_sampler(worker: EventQueueWorker, stats: SustainedStats, stop_at: float) -> None:
    while time.monotonic() < stop_at:
        stats.add_queue_size(worker.queue_size())
        time.sleep(0.2)


def _sustained_worker(
    worker_id: int,
    worker: EventQueueWorker,
    settings: QueueBenchSettings,
    stats: SustainedStats,
    stop_at: float,
    requests_per_second_per_worker: float,
) -> None:
    seed = settings.generator_seed + (worker_id * 100000)
    next_due_at = time.monotonic()

    while time.monotonic() < stop_at:
        now = time.monotonic()
        if now < next_due_at:
            time.sleep(next_due_at - now)
        next_due_at += 1.0 / requests_per_second_per_worker

        payload = _build_request_payload(settings.total_sessions_per_request, seed)
        seed += 1
        try:
            worker.enqueue_many(payload)
            stats.add_request(len(payload), accepted=True)
        except QueueFullError:
            stats.add_request(len(payload), accepted=False)


def _run_sustained(settings: QueueBenchSettings, events_per_request: int) -> dict[str, Any]:
    sink = FakeOpenSearchService(bulk_latency_seconds=settings.bulk_latency_ms / 1000.0)
    worker = _make_queue_worker(settings, sink)
    worker.start()

    stats = SustainedStats()
    started_at = time.monotonic()
    stop_at = started_at + settings.sustained_duration_seconds
    target_rps = settings.target_events_per_second / max(1, events_per_request)
    requests_per_second_per_worker = target_rps / settings.enqueue_concurrency

    sampler = threading.Thread(
        target=_queue_sampler,
        args=(worker, stats, stop_at),
        daemon=True,
    )
    sampler.start()

    threads = [
        threading.Thread(
            target=_sustained_worker,
            args=(worker_id, worker, settings, stats, stop_at, requests_per_second_per_worker),
            daemon=True,
        )
        for worker_id in range(settings.enqueue_concurrency)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    sampler.join(timeout=1.0)

    queue_size_before_stop = worker.queue_size()
    worker.stop(flush_remaining=True)
    total_wall_seconds = max(0.001, time.monotonic() - started_at)
    active_window_seconds = max(0.001, settings.sustained_duration_seconds)
    drain_seconds_after_window = max(0.0, total_wall_seconds - active_window_seconds)

    return {
        "target_events_per_second": settings.target_events_per_second,
        "target_requests_per_second": round(target_rps, 3),
        "active_window_seconds": active_window_seconds,
        "total_wall_seconds": round(total_wall_seconds, 4),
        "drain_seconds_after_window": round(drain_seconds_after_window, 4),
        "accepted_requests": stats.accepted_requests,
        "rejected_requests": stats.rejected_requests,
        "accepted_events": stats.accepted_events,
        "rejected_events": stats.rejected_events,
        "accepted_events_per_target_window": round(stats.accepted_events / active_window_seconds, 2),
        "completed_events_per_total_wall_second": round(stats.accepted_events / total_wall_seconds, 2),
        "queue_size": {
            "avg": round(mean(stats.queue_sizes), 2) if stats.queue_sizes else 0.0,
            "max": max(stats.queue_sizes) if stats.queue_sizes else 0,
            "before_stop": queue_size_before_stop,
        },
        "downstream": {
            "simulated_bulk_latency_ms": settings.bulk_latency_ms,
            "bulk_calls": sink.bulk_calls,
            "bulk_events": sink.bulk_events,
            "flush_events_per_total_wall_second": round(sink.bulk_events / total_wall_seconds, 2),
        },
    }


def _run_drain(settings: QueueBenchSettings, payload_shape: dict[str, Any]) -> dict[str, Any]:
    sink = FakeOpenSearchService(bulk_latency_seconds=settings.bulk_latency_ms / 1000.0)
    worker = _make_queue_worker(settings, sink)
    seed = settings.generator_seed

    queued_events = 0
    while queued_events < settings.prefill_events_for_drain:
        payload = _build_request_payload(settings.total_sessions_per_request, seed)
        seed += 1
        remaining_capacity = settings.queue_max_size - queued_events
        if remaining_capacity <= 0:
            break
        slice_count = min(len(payload), remaining_capacity, settings.prefill_events_for_drain - queued_events)
        worker.enqueue_many(copy.deepcopy(payload[:slice_count]))
        queued_events += slice_count

    started_at = time.monotonic()
    worker.start()
    while worker.queue_size() > 0:
        time.sleep(0.05)
    elapsed_seconds = max(0.001, time.monotonic() - started_at)
    worker.stop(flush_remaining=True)

    return {
        "prefilled_events": queued_events,
        "drain_seconds": round(elapsed_seconds, 4),
        "drain_events_per_second": round(queued_events / elapsed_seconds, 2),
        "bulk_calls": sink.bulk_calls,
        "bulk_events": sink.bulk_events,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark the in-memory ingestion queue without OpenSearch.",
    )
    parser.add_argument("--total-sessions-per-request", type=int, default=7)
    parser.add_argument("--enqueue-concurrency", type=int, default=4)
    parser.add_argument("--sustained-duration-seconds", type=int, default=10)
    parser.add_argument("--target-events-per-second", type=int, default=1000)
    parser.add_argument("--bulk-latency-ms", type=float, default=50.0)
    parser.add_argument("--queue-batch-size", type=int, default=100)
    parser.add_argument("--queue-flush-interval-seconds", type=float, default=2.0)
    parser.add_argument("--queue-max-size", type=int, default=10000)
    parser.add_argument("--prefill-events-for-drain", type=int, default=5000)
    parser.add_argument("--generator-seed", type=int, default=20000)
    args = parser.parse_args()

    settings = QueueBenchSettings(
        total_sessions_per_request=args.total_sessions_per_request,
        enqueue_concurrency=args.enqueue_concurrency,
        sustained_duration_seconds=args.sustained_duration_seconds,
        target_events_per_second=args.target_events_per_second,
        bulk_latency_ms=args.bulk_latency_ms,
        queue_batch_size=args.queue_batch_size,
        queue_flush_interval_seconds=args.queue_flush_interval_seconds,
        queue_max_size=args.queue_max_size,
        prefill_events_for_drain=args.prefill_events_for_drain,
        generator_seed=args.generator_seed,
    )

    payload_shape = _sample_payload_shape(
        settings.total_sessions_per_request,
        settings.generator_seed,
    )
    estimated_queue_heap_megabytes = (
        payload_shape["avg_event_heap_bytes"] * settings.queue_max_size / (1024 * 1024)
    )

    result = {
        "queue_settings": {
            "batch_size": settings.queue_batch_size,
            "flush_interval_seconds": settings.queue_flush_interval_seconds,
            "max_queue_size": settings.queue_max_size,
            "bulk_latency_ms": settings.bulk_latency_ms,
        },
        "payload_shape": {
            **payload_shape,
            "estimated_max_queue_heap_megabytes": round(estimated_queue_heap_megabytes, 2),
        },
        "burst_fill": _run_burst_fill(settings, payload_shape["events_per_request"]),
        "sustained": _run_sustained(settings, payload_shape["events_per_request"]),
        "drain": _run_drain(settings, payload_shape),
    }

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
