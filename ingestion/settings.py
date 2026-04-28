from __future__ import annotations

import os
from dataclasses import dataclass

from common.parsing import parse_bool


@dataclass(frozen=True)
class IngestionSettings:
    app_host: str
    app_port: int
    opensearch_url: str
    clickstream_index: str
    dead_letter_index: str
    data_quality_index: str
    quality_checkpoint_index: str
    batch_size: int
    flush_interval_seconds: float
    max_queue_size: int
    opensearch_timeout_seconds: float
    startup_retries: int
    startup_retry_delay_seconds: float
    log_invalid_payloads: bool
    late_arrival_threshold_seconds: float

    @classmethod
    def from_env(cls) -> "IngestionSettings":
        return cls(
            app_host=os.getenv("INGESTION_HOST", "0.0.0.0"),
            app_port=int(os.getenv("INGESTION_PORT", "8000")),
            opensearch_url=os.getenv("OPENSEARCH_URL", "http://opensearch:9200"),
            clickstream_index=os.getenv("CLICKSTREAM_INDEX", "clickstream-events"),
            dead_letter_index=os.getenv("DEAD_LETTER_INDEX", "dead-letter-events"),
            data_quality_index=os.getenv("DATA_QUALITY_INDEX", "data-quality-results"),
            quality_checkpoint_index=os.getenv(
                "QUALITY_CHECKPOINT_INDEX", "quality-checkpoints"
            ),
            batch_size=int(os.getenv("BATCH_SIZE", "100")),
            flush_interval_seconds=float(os.getenv("FLUSH_INTERVAL_SECONDS", "2")),
            max_queue_size=int(os.getenv("MAX_QUEUE_SIZE", "10000")),
            opensearch_timeout_seconds=float(os.getenv("OPENSEARCH_TIMEOUT_SECONDS", "10")),
            startup_retries=int(os.getenv("OPENSEARCH_STARTUP_RETRIES", "30")),
            startup_retry_delay_seconds=float(
                os.getenv("OPENSEARCH_STARTUP_RETRY_DELAY_SECONDS", "2")
            ),
            log_invalid_payloads=parse_bool(os.getenv("LOG_INVALID_PAYLOADS"), default=True),
            late_arrival_threshold_seconds=float(
                os.getenv("LATE_ARRIVAL_THRESHOLD_SECONDS", "3600")
            ),
        )


settings = IngestionSettings.from_env()
