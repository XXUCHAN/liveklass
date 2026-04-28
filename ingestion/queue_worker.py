from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from ingestion.settings import IngestionSettings, settings
from storage.opensearch_client import OpenSearchService, build_dead_letter_document


class QueueFullError(Exception):
    """Raised when the in-memory event queue exceeds the configured limit."""


@dataclass(frozen=True)
class FlushResult:
    attempted: int
    succeeded: int
    failed: int


class EventQueueWorker:
    def __init__(
        self,
        opensearch_service: OpenSearchService,
        app_settings: IngestionSettings | None = None,
    ) -> None:
        self.settings = app_settings or settings
        self.opensearch_service = opensearch_service
        self._queue: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._flush_signal = threading.Event()
        self._stop_signal = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return

        self._stop_signal.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="event-queue-worker",
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, flush_remaining: bool = True) -> None:
        self._stop_signal.set()
        self._flush_signal.set()

        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.settings.flush_interval_seconds + 1))
            self._thread = None

        if flush_remaining:
            self.flush_all()

    def queue_size(self) -> int:
        with self._lock:
            return len(self._queue)

    def enqueue(self, event: dict[str, Any]) -> None:
        self.enqueue_many([event])

    def enqueue_many(self, events: list[dict[str, Any]]) -> None:
        if not events:
            return

        with self._lock:
            projected_size = len(self._queue) + len(events)
            if projected_size > self.settings.max_queue_size:
                raise QueueFullError(
                    "event queue is full "
                    f"({projected_size}>{self.settings.max_queue_size})"
                )

            self._queue.extend(events)
            should_flush_early = len(self._queue) >= self.settings.batch_size

        if should_flush_early:
            self._flush_signal.set()

    def flush_once(self) -> FlushResult:
        batch = self._pop_batch(self.settings.batch_size)
        if not batch:
            return FlushResult(attempted=0, succeeded=0, failed=0)

        try:
            succeeded = self.opensearch_service.bulk_index_events(batch)
            failed = max(0, len(batch) - succeeded)

            if failed > 0:
                print(
                    "Warning: OpenSearch bulk indexing did not confirm all events "
                    f"({succeeded}/{len(batch)} succeeded)."
                )

            return FlushResult(
                attempted=len(batch),
                succeeded=succeeded,
                failed=failed,
            )
        except Exception as exc:
            self._dead_letter_batch(batch, f"bulk insert failed: {exc}")
            return FlushResult(
                attempted=len(batch),
                succeeded=0,
                failed=len(batch),
            )

    def flush_all(self) -> FlushResult:
        total_attempted = 0
        total_succeeded = 0
        total_failed = 0

        while self.queue_size() > 0:
            result = self.flush_once()
            if result.attempted == 0:
                break

            total_attempted += result.attempted
            total_succeeded += result.succeeded
            total_failed += result.failed

        return FlushResult(
            attempted=total_attempted,
            succeeded=total_succeeded,
            failed=total_failed,
        )

    def _pop_batch(self, batch_size: int) -> list[dict[str, Any]]:
        with self._lock:
            if not self._queue:
                return []

            batch = self._queue[:batch_size]
            del self._queue[:batch_size]
            return batch

    def _dead_letter_batch(self, batch: list[dict[str, Any]], error_reason: str) -> None:
        dead_letters = [
            build_dead_letter_document(
                payload=event,
                error_reason=error_reason,
                failed_stage="bulk_insert",
            )
            for event in batch
        ]
        try:
            self.opensearch_service.bulk_index_dead_letters(dead_letters)
        except Exception as dead_letter_error:
            print(f"Failed to write dead-letter batch: {dead_letter_error}")

    def _run(self) -> None:
        while not self._stop_signal.is_set():
            self._flush_signal.wait(timeout=self.settings.flush_interval_seconds)
            self._flush_signal.clear()
            self.flush_once()

        if self.queue_size() > 0:
            self.flush_all()
