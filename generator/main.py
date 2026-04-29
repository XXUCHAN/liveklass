from __future__ import annotations

import json
import os
import random
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx

try:
    from common.parsing import format_timestamp, parse_bool, parse_timestamp
    from generator.click_model import (
        POPULARITY_BOOST,
        PRESENTATION_BOOST,
        POSITION_BIAS,
        calculate_click_probability,
        get_position_bias,
    )
except ModuleNotFoundError:
    import sys

    PROJECT_ROOT = Path(__file__).resolve().parent.parent
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    from common.parsing import format_timestamp, parse_bool, parse_timestamp  # type: ignore[no-redef]
    from click_model import (
        POPULARITY_BOOST,
        PRESENTATION_BOOST,
        POSITION_BIAS,
        calculate_click_probability,
        get_position_bias,
    )


EVENT_TYPES = {"page_view", "impression", "click", "purchase", "error"}
DEVICES = ("mobile", "desktop", "tablet")
PAGE_URLS = ("/", "/courses", "/search", "/checkout")
ERROR_CATALOG = (
    ("PAYMENT_FAILED", "payment approval failed"),
    ("SEARCH_TIMEOUT", "search request timed out"),
    ("PLAYER_INIT_FAILED", "video player failed to initialize"),
)
QUERY_PROFILES = (
    ("python course", "programming"),
    ("data engineering", "data"),
    ("sql bootcamp", "data"),
    ("product design", "design"),
    ("cloud fundamentals", "cloud"),
)
PRESENTATION_WEIGHTS = (
    ("normal_card", 0.55),
    ("featured_card", 0.15),
    ("discount_badge", 0.18),
    ("live_badge", 0.12),
)


@dataclass(frozen=True)
class GeneratorSettings:
    api_url: str
    send_to_api: bool
    mode: str
    total_sessions: int
    batch_size: int
    catalog_size: int
    max_results_per_page: int
    max_clicks_per_session: int
    purchase_rate: float
    error_rate: float
    request_timeout_seconds: float
    health_retries: int
    retry_delay_seconds: float
    cycle_interval_seconds: float
    max_cycles: int
    seed: int
    ensure_all_event_types: bool
    # TODO: Add an invalid_event_rate setting

    @classmethod
    def from_env(cls) -> "GeneratorSettings":
        mode = os.getenv("GENERATOR_MODE", "dev").strip().lower()
        if mode not in {"dev", "prod"}:
            mode = "dev"

        return cls(
            api_url=os.getenv("GENERATOR_API_URL", "http://api:8000/events"),
            send_to_api=parse_bool(os.getenv("SEND_TO_API"), default=(mode == "prod")),
            mode=mode,
            total_sessions=int(os.getenv("TOTAL_SESSIONS", "120")),
            batch_size=int(os.getenv("GENERATOR_BATCH_SIZE", "100")),
            catalog_size=int(os.getenv("CATALOG_SIZE", "60")),
            max_results_per_page=int(os.getenv("MAX_RESULTS_PER_PAGE", "10")),
            max_clicks_per_session=int(os.getenv("MAX_CLICKS_PER_SESSION", "2")),
            purchase_rate=float(os.getenv("PURCHASE_RATE", "0.22")),
            error_rate=float(os.getenv("ERROR_RATE", "0.05")),
            request_timeout_seconds=float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10")),
            health_retries=int(os.getenv("API_HEALTH_RETRIES", "30")),
            retry_delay_seconds=float(os.getenv("API_RETRY_DELAY_SECONDS", "2")),
            cycle_interval_seconds=float(os.getenv("GENERATOR_INTERVAL_SECONDS", "60")),
            max_cycles=int(os.getenv("GENERATOR_MAX_CYCLES", "0")),
            seed=int(os.getenv("GENERATOR_SEED", "50")),
            ensure_all_event_types=parse_bool(os.getenv("ENSURE_ALL_EVENT_TYPES"), default=True),
        )

    @property
    def health_url(self) -> str:
        if self.api_url.endswith("/events"):
            return f"{self.api_url[:-7]}/health"
        return f"{self.api_url.rstrip('/')}/health"


@dataclass(frozen=True)
class CatalogItem:
    item_id: str
    title: str
    category: str
    popularity_bucket: str
    base_relevance: float
    price: int


class ClickstreamGenerator:
    def __init__(self, settings: GeneratorSettings) -> None:
        self.settings = settings
        self.random = random.Random(settings.seed)
        self.catalog = self._build_catalog(settings.catalog_size)
        self.user_count = max(50, settings.total_sessions // 2)

    def generate(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        session_anchor = datetime.now(UTC) - timedelta(minutes=max(1, self.settings.total_sessions // 4))

        for index in range(self.settings.total_sessions):
            session_start = session_anchor + timedelta(seconds=index * self.random.randint(15, 45))
            user_id = f"user_{self.random.randint(1, self.user_count):03d}"
            session_id = f"session_{uuid4().hex[:12]}"
            device = self.random.choices(DEVICES, weights=(0.55, 0.35, 0.10), k=1)[0]
            query, category = self.random.choice(QUERY_PROFILES)
            results = self._rank_results(category)
            events.extend(
                self._generate_session_events(
                    user_id=user_id,
                    session_id=session_id,
                    device=device,
                    query=query,
                    results=results,
                    session_start=session_start,
                )
            )

        if self.settings.ensure_all_event_types:
            self._ensure_event_type_coverage(events)

        # TODO: events for validation/DLQ
        return sorted(events, key=lambda event: event["event_time"])

    def _build_catalog(self, size: int) -> list[CatalogItem]:
        categories = ["programming", "data", "design", "cloud", "marketing"]
        titles = {
            "programming": ["Python Fundamentals", "Backend API Design", "Async Python"],
            "data": ["Data Engineering Bootcamp", "SQL Analytics", "Streaming Pipelines"],
            "design": ["UI Design Basics", "Product Design Sprint", "Figma for Teams"],
            "cloud": ["AWS Starter", "Kubernetes Basics", "Cloud Architecture"],
            "marketing": ["Growth Tactics", "Performance Ads", "CRM Automation"],
        }
        popularity_weights = (("low", 0.25), ("medium", 0.50), ("high", 0.25))
        catalog: list[CatalogItem] = []

        for index in range(1, size + 1):
            category = categories[(index - 1) % len(categories)]
            title = self.random.choice(titles[category])
            popularity_bucket = self.random.choices(
                [item[0] for item in popularity_weights],
                weights=[item[1] for item in popularity_weights],
                k=1,
            )[0]
            base_relevance = round(self.random.uniform(0.35, 0.9), 3)
            price = self.random.choice([19000, 29000, 39000, 59000, 79000])
            catalog.append(
                CatalogItem(
                    item_id=f"course_{index:03d}",
                    title=title,
                    category=category,
                    popularity_bucket=popularity_bucket,
                    base_relevance=base_relevance,
                    price=price,
                )
            )
        return catalog

    def _rank_results(self, query_category: str) -> list[CatalogItem]:
        scored: list[tuple[float, CatalogItem]] = []
        for item in self.catalog:
            category_boost = 0.18 if item.category == query_category else -0.10
            popularity_adjustment = (POPULARITY_BOOST[item.popularity_bucket] - 1.0) * 0.12
            noise = self.random.uniform(-0.08, 0.08)
            score = item.base_relevance + category_boost + popularity_adjustment + noise
            scored.append((score, item))

        scored.sort(key=lambda entry: entry[0], reverse=True)
        top_results = [item for _, item in scored[: self.settings.max_results_per_page]]
        return top_results

    def _generate_session_events(
        self,
        *,
        user_id: str,
        session_id: str,
        device: str,
        query: str,
        results: list[CatalogItem],
        session_start: datetime,
    ) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        page_url = f"/courses?query={query.replace(' ', '+')}"
        page_view_time = session_start
        events.append(
            self._build_common_event(
                event_type="page_view",
                user_id=user_id,
                session_id=session_id,
                device=device,
                event_time=page_view_time,
                page_url=page_url,
            )
        )

        clicks_in_session = 0

        for rank, item in enumerate(results, start=1):
            presentation_type = self.random.choices(
                [item[0] for item in PRESENTATION_WEIGHTS],
                weights=[item[1] for item in PRESENTATION_WEIGHTS],
                k=1,
            )[0]
            relevance = self._session_relevance(item)
            impression_time = page_view_time + timedelta(seconds=rank * self.random.uniform(0.7, 1.4))
            impression_event = self._build_common_event(
                event_type="impression",
                user_id=user_id,
                session_id=session_id,
                device=device,
                event_time=impression_time,
                query=query,
                item_id=item.item_id,
                rank=rank,
                popularity_bucket=item.popularity_bucket,
                presentation_type=presentation_type,
                position_bias=get_position_bias(rank),
            )
            events.append(impression_event)

            click_prob = calculate_click_probability(
                base_relevance=relevance,
                rank=rank,
                popularity_bucket=item.popularity_bucket,
                presentation_type=presentation_type,
            )
            should_click = (
                clicks_in_session < self.settings.max_clicks_per_session
                and self.random.random() < click_prob
            )

            if not should_click:
                continue

            clicks_in_session += 1
            click_time = impression_time + timedelta(seconds=self.random.uniform(0.4, 1.6))
            click_event = self._build_common_event(
                event_type="click",
                user_id=user_id,
                session_id=session_id,
                device=device,
                event_time=click_time,
                query=query,
                item_id=item.item_id,
                rank=rank,
                popularity_bucket=item.popularity_bucket,
                presentation_type=presentation_type,
                position_bias=get_position_bias(rank),
                click_prob=click_prob,
            )
            events.append(click_event)

            if self.random.random() < self.settings.purchase_rate:
                purchase_time = click_time + timedelta(seconds=self.random.uniform(5, 25))
                events.append(
                    self._build_common_event(
                        event_type="purchase",
                        user_id=user_id,
                        session_id=session_id,
                        device=device,
                        event_time=purchase_time,
                        item_id=item.item_id,
                        amount=item.price,
                    )
                )

        if self.random.random() < self.settings.error_rate:
            error_code, error_message = self.random.choice(ERROR_CATALOG)
            error_time = page_view_time + timedelta(seconds=self.random.uniform(1, 30))
            events.append(
                self._build_common_event(
                    event_type="error",
                    user_id=user_id,
                    session_id=session_id,
                    device=device,
                    event_time=error_time,
                    error_code=error_code,
                    error_message=error_message,
                )
            )

        return events

    def _session_relevance(self, item: CatalogItem) -> float:
        relevance = item.base_relevance + self.random.uniform(-0.05, 0.05)
        return round(max(0.2, min(relevance, 0.95)), 4)

    def _build_common_event(
        self,
        *,
        event_type: str,
        user_id: str,
        session_id: str,
        device: str,
        event_time: datetime,
        **fields: Any,
    ) -> dict[str, Any]:
        received_at = event_time + timedelta(milliseconds=self.random.randint(150, 2000))
        return {
            "event_id": str(uuid4()),
            "schema_version": "1.0",
            "event_type": event_type,
            "user_id": user_id,
            "session_id": session_id,
            "event_time": format_timestamp(event_time),
            "received_at": format_timestamp(received_at),
            "device": device,
            **fields,
        }
    # DEMO용 보정
    def _ensure_event_type_coverage(self, events: list[dict[str, Any]]) -> None:
        counts = Counter(event["event_type"] for event in events)
        if not events:
            return

        first_event = events[0]
        anchor_time = parse_timestamp(first_event["event_time"])
        if anchor_time is None:
            anchor_time = datetime.now(UTC)
        fallback_user = first_event["user_id"]
        fallback_session = first_event["session_id"]
        fallback_device = first_event["device"]

        if counts["error"] == 0:
            events.append(
                self._build_common_event(
                    event_type="error",
                    user_id=fallback_user,
                    session_id=fallback_session,
                    device=fallback_device,
                    event_time=anchor_time + timedelta(seconds=1),
                    error_code="PAYMENT_FAILED",
                    error_message="payment approval failed",
                )
            )

        if counts["purchase"] == 0:
            click_event = next((event for event in events if event["event_type"] == "click"), None)
            if click_event is not None:
                click_time = parse_timestamp(click_event["event_time"])
                if click_time is None:
                    click_time = anchor_time
                events.append(
                    self._build_common_event(
                        event_type="purchase",
                        user_id=click_event["user_id"],
                        session_id=click_event["session_id"],
                        device=click_event["device"],
                        event_time=click_time + timedelta(seconds=8),
                        item_id=click_event["item_id"],
                        amount=39000,
                    )
                )

    def post_events(self, events: list[dict[str, Any]]) -> None:
        self._wait_for_api()

        with httpx.Client(timeout=self.settings.request_timeout_seconds) as client:
            for batch_start in range(0, len(events), self.settings.batch_size):
                batch = events[batch_start : batch_start + self.settings.batch_size]
                response = client.post(self.settings.api_url, json=batch)
                response.raise_for_status()

    def _wait_for_api(self) -> None:
        for attempt in range(1, self.settings.health_retries + 1):
            try:
                response = httpx.get(self.settings.health_url, timeout=self.settings.request_timeout_seconds)
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass

            print(
                f"Waiting for ingestion API ({attempt}/{self.settings.health_retries}) at {self.settings.health_url}"
            )
            time.sleep(self.settings.retry_delay_seconds)

        raise RuntimeError(f"ingestion API is not reachable at {self.settings.health_url}")


def summarize_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(event["event_type"] for event in events)
    sample_events = events[:3]
    return {
        "total_events": len(events),
        "event_type_counts": dict(sorted(counts.items())),
        "sample_events": sample_events,
        "bias_reference": {
            "position_bias": POSITION_BIAS,
            "popularity_boost": POPULARITY_BOOST,
            "presentation_boost": PRESENTATION_BOOST,
        },
    }


def run_oneshot(settings: GeneratorSettings) -> None:
    generator = ClickstreamGenerator(settings)
    events = generator.generate()
    summary = summarize_events(events)

    if settings.send_to_api:
        generator.post_events(events)
        print(
            json.dumps(
                {
                    **summary,
                    "delivery": "posted_to_api",
                    "mode": settings.mode,
                    "execution_pattern": "oneshot",
                },
                indent=2,
            )
        )
        return

    print(
        json.dumps(
            {
                **summary,
                "delivery": "dry_run",
                "mode": settings.mode,
                "execution_pattern": "oneshot",
            },
            indent=2,
        )
    )


def run_continuous(settings: GeneratorSettings) -> None:
    generator = ClickstreamGenerator(settings)
    cycle = 0

    while True:
        cycle += 1
        events = generator.generate()
        summary = summarize_events(events)

        if settings.send_to_api:
            generator.post_events(events)
            delivery = "posted_to_api"
        else:
            delivery = "dry_run"

        print(
            json.dumps(
                {
                    **summary,
                    "delivery": delivery,
                    "mode": settings.mode,
                    "execution_pattern": "continuous",
                    "cycle": cycle,
                    "sleep_seconds": settings.cycle_interval_seconds,
                },
                indent=2,
            )
        )

        if settings.max_cycles > 0 and cycle >= settings.max_cycles:
            return

        time.sleep(settings.cycle_interval_seconds)


def main() -> None:
    settings = GeneratorSettings.from_env()

    if settings.mode == "prod":
        run_continuous(settings)
        return

    run_oneshot(settings)


if __name__ == "__main__":
    main()
