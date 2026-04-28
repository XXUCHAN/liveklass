from __future__ import annotations

import time
from typing import Any
from uuid import uuid4

from opensearchpy import OpenSearch, helpers
from opensearchpy.exceptions import NotFoundError

from common.parsing import utc_timestamp
from ingestion.settings import IngestionSettings, settings


def _base_index_settings() -> dict[str, Any]:
    return {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
            }
        }
    }


def _clickstream_index_mapping() -> dict[str, Any]:
    return {
        **_base_index_settings(),
        "mappings": {
            "properties": {
                "event_id": {"type": "keyword"},
                "schema_version": {"type": "keyword"},
                "event_type": {"type": "keyword"},
                "user_id": {"type": "keyword"},
                "session_id": {"type": "keyword"},
                "device": {"type": "keyword"},
                "page_url": {"type": "keyword"},
                "query": {"type": "keyword"},
                "item_id": {"type": "keyword"},
                "rank": {"type": "integer"},
                "popularity_bucket": {"type": "keyword"},
                "presentation_type": {"type": "keyword"},
                "position_bias": {"type": "float"},
                "click_prob": {"type": "float"},
                "amount": {"type": "float"},
                "error_code": {"type": "keyword"},
                "error_message": {"type": "text"},
                "event_time": {"type": "date"},
                "received_at": {"type": "date"},
                "ingested_at": {"type": "date"},
                "arrival_lag_seconds": {"type": "float"},
                "is_late_arrival": {"type": "boolean"},
            }
        },
    }


def _dead_letter_index_mapping() -> dict[str, Any]:
    return {
        **_base_index_settings(),
        "mappings": {
            "properties": {
                "event_id": {"type": "keyword"},
                "payload": {"type": "object", "enabled": True},
                "error_reason": {"type": "text"},
                "failed_stage": {"type": "keyword"},
                "created_at": {"type": "date"},
            }
        },
    }


def _data_quality_index_mapping() -> dict[str, Any]:
    return {
        **_base_index_settings(),
        "mappings": {
            "properties": {
                "check_name": {"type": "keyword"},
                "status": {"type": "keyword"},
                "failed_count": {"type": "integer"},
                "checked_at": {"type": "date"},
                "details": {"type": "object", "enabled": True},
            }
        },
    }


def _quality_checkpoint_index_mapping() -> dict[str, Any]:
    return {
        **_base_index_settings(),
        "mappings": {
            "properties": {
                "job_name": {"type": "keyword"},
                "last_checked_ingested_at": {"type": "date"},
                "last_checked_at": {"type": "date"},
                "last_run_cutoff": {"type": "date"},
                "overlap_window_seconds": {"type": "integer"},
                "last_incremental_count": {"type": "integer"},
                "last_window_count": {"type": "integer"},
            }
        },
    }


class OpenSearchService:
    def __init__(self, app_settings: IngestionSettings) -> None:
        self.settings = app_settings
        self.client = OpenSearch(
            hosts=[app_settings.opensearch_url],
            use_ssl=False,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            timeout=app_settings.opensearch_timeout_seconds,
        )

    def wait_until_ready(self) -> None:
        for attempt in range(1, self.settings.startup_retries + 1):
            try:
                if self.client.ping():
                    return
            except Exception:
                pass

            print(
                "Waiting for OpenSearch "
                f"({attempt}/{self.settings.startup_retries}) at {self.settings.opensearch_url}"
            )
            time.sleep(self.settings.startup_retry_delay_seconds)

        raise RuntimeError(f"OpenSearch is not reachable at {self.settings.opensearch_url}")

    def ensure_indexes(self) -> None:
        self._ensure_index(self.settings.clickstream_index, _clickstream_index_mapping())
        self._ensure_index(self.settings.dead_letter_index, _dead_letter_index_mapping())
        self._ensure_index(self.settings.data_quality_index, _data_quality_index_mapping())
        self._ensure_index(
            self.settings.quality_checkpoint_index,
            _quality_checkpoint_index_mapping(),
        )

    def _ensure_index(self, index_name: str, body: dict[str, Any]) -> None:
        if self.client.indices.exists(index=index_name):
            return
        self.client.indices.create(index=index_name, body=body)

    def healthcheck(self) -> bool:
        try:
            return bool(self.client.ping())
        except Exception:
            return False

    def bulk_index_events(self, events: list[dict[str, Any]]) -> int:
        return self.bulk_index_documents(self.settings.clickstream_index, events)

    def bulk_index_dead_letters(self, documents: list[dict[str, Any]]) -> int:
        return self.bulk_index_documents(self.settings.dead_letter_index, documents)

    def bulk_index_documents(self, index_name: str, documents: list[dict[str, Any]]) -> int:
        if not documents:
            return 0

        actions = [
            {
                "_index": index_name,
                "_source": document,
            }
            for document in documents
        ]
        success_count, _ = helpers.bulk(
            self.client,
            actions,
            raise_on_error=False,
            raise_on_exception=False,
            request_timeout=self.settings.opensearch_timeout_seconds,
        )
        return int(success_count)

    def index_dead_letter(
        self,
        *,
        payload: dict[str, Any] | Any,
        error_reason: str,
        failed_stage: str = "validation",
        event_id: str | None = None,
    ) -> dict[str, Any]:
        document = build_dead_letter_document(
            payload=payload,
            error_reason=error_reason,
            failed_stage=failed_stage,
            event_id=event_id,
        )
        self.client.index(index=self.settings.dead_letter_index, body=document)
        return document

    def index_quality_result(self, document: dict[str, Any]) -> dict[str, Any]:
        self.client.index(index=self.settings.data_quality_index, body=document)
        return document

    def get_quality_checkpoint(self, job_name: str) -> dict[str, Any] | None:
        try:
            response = self.client.get(index=self.settings.quality_checkpoint_index, id=job_name)
        except NotFoundError:
            return None
        return response.get("_source")

    def upsert_quality_checkpoint(self, job_name: str, document: dict[str, Any]) -> dict[str, Any]:
        payload = {"job_name": job_name, **document}
        self.client.index(
            index=self.settings.quality_checkpoint_index,
            id=job_name,
            body=payload,
            refresh=True,
        )
        return payload


def build_dead_letter_document(
    *,
    payload: dict[str, Any] | Any,
    error_reason: str,
    failed_stage: str,
    event_id: str | None = None,
) -> dict[str, Any]:
    resolved_event_id = event_id
    if resolved_event_id is None and isinstance(payload, dict):
        payload_event_id = payload.get("event_id")
        if isinstance(payload_event_id, str) and payload_event_id.strip():
            resolved_event_id = payload_event_id

    return {
        "event_id": resolved_event_id or f"missing-event-id-{uuid4().hex[:12]}",
        "payload": payload if isinstance(payload, dict) else {"raw_payload": payload},
        "error_reason": error_reason,
        "failed_stage": failed_stage,
        "created_at": utc_timestamp(),
    }


def create_opensearch_service(app_settings: IngestionSettings | None = None) -> OpenSearchService:
    return OpenSearchService(app_settings or settings)

