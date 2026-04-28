from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import Body, FastAPI, HTTPException, Response

from storage.opensearch_client import (
    OpenSearchService,
    build_dead_letter_document,
    create_opensearch_service,
)
from ingestion.queue_worker import EventQueueWorker, QueueFullError
from ingestion.settings import IngestionSettings, settings
from ingestion.validation import validate_event


class IngestionRuntime:
    def __init__(self, app_settings: IngestionSettings) -> None:
        self.settings = app_settings
        self.opensearch_service: OpenSearchService = create_opensearch_service(app_settings)
        self.queue_worker = EventQueueWorker(self.opensearch_service, app_settings)
        self.ready = False

    def startup(self) -> None:
        self.opensearch_service.wait_until_ready()
        self.opensearch_service.ensure_indexes()
        self.queue_worker.start()
        self.ready = True

    def shutdown(self) -> None:
        self.queue_worker.stop(flush_remaining=True)
        self.ready = False


runtime = IngestionRuntime(settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    runtime.startup()
    app.state.runtime = runtime
    yield
    runtime.shutdown()


app = FastAPI(
    title="Clickstream Ingestion Service",
    version="1.0.0",
    lifespan=lifespan,
)


def _get_runtime() -> IngestionRuntime:
    return runtime


def _normalize_payloads(payload: Any) -> list[Any]:
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return payload
    raise HTTPException(status_code=400, detail="payload must be an object or a list of objects")


def _payload_for_dead_letter(raw_event: Any, app_settings: IngestionSettings) -> dict[str, Any] | Any:
    if app_settings.log_invalid_payloads:
        return raw_event

    if isinstance(raw_event, dict):
        event_id = raw_event.get("event_id")
        if isinstance(event_id, str) and event_id.strip():
            return {"event_id": event_id, "payload_redacted": True}
    return {"payload_redacted": True}


@app.get("/health")
async def health(response: Response) -> dict[str, Any]:
    current_runtime = _get_runtime()
    opensearch_up = current_runtime.opensearch_service.healthcheck()
    is_healthy = current_runtime.ready and opensearch_up
    response.status_code = 200 if is_healthy else 503

    return {
        "status": "ok" if is_healthy else "degraded",
        "service": "ingestion",
        "opensearch": "up" if opensearch_up else "down",
        "queue_size": current_runtime.queue_worker.queue_size(),
        "ready": current_runtime.ready,
    }


@app.post("/events")
async def ingest_events(payload: Any = Body(...)) -> dict[str, Any]:
    current_runtime = _get_runtime()
    raw_events = _normalize_payloads(payload)

    if not raw_events:
        raise HTTPException(status_code=400, detail="payload list must not be empty")

    valid_events: list[dict[str, Any]] = []
    dead_letter_documents: list[dict[str, Any]] = []

    for raw_event in raw_events:
        validation_result = validate_event(
            raw_event,
            late_arrival_threshold_seconds=current_runtime.settings.late_arrival_threshold_seconds,
        )
        if validation_result.is_valid and validation_result.normalized_event is not None:
            valid_events.append(validation_result.normalized_event)
            continue

        error_reason = "; ".join(validation_result.errors) if validation_result.errors else "validation failed"
        dead_letter_documents.append(
            build_dead_letter_document(
                payload=_payload_for_dead_letter(raw_event, current_runtime.settings),
                error_reason=error_reason,
                failed_stage="validation",
                event_id=raw_event.get("event_id") if isinstance(raw_event, dict) else None,
            )
        )

    dead_lettered = 0
    if dead_letter_documents:
        try:
            dead_lettered = current_runtime.opensearch_service.bulk_index_dead_letters(
                dead_letter_documents
            )
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=f"failed to store invalid events in dead-letter index: {exc}",
            ) from exc

    if valid_events:
        try:
            current_runtime.queue_worker.enqueue_many(valid_events)
        except QueueFullError as exc:
            raise HTTPException(status_code=429, detail="event queue is full") from exc

    return {
        "status": "accepted",
        "received": len(raw_events),
        "accepted": len(valid_events),
        "invalid": len(dead_letter_documents),
        "dead_lettered": dead_lettered,
        "queue_size": current_runtime.queue_worker.queue_size(),
    }


def main() -> None:
    uvicorn.run(
        app,
        host=settings.app_host,
        port=settings.app_port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
