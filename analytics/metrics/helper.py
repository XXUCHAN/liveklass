from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from common.parsing import coerce_int


@dataclass(frozen=True)
class ImpressionSample:
    rank: int
    device: str
    popularity_bucket: str
    presentation_type: str
    clicked: int


@dataclass(frozen=True)
class LogisticClickModel:
    rank_values: tuple[int, ...]
    rank_mean: float
    rank_std: float
    device_values: tuple[str, ...]
    popularity_values: tuple[str, ...]
    presentation_values: tuple[str, ...]
    device_baseline: str
    popularity_baseline: str
    presentation_baseline: str
    intercept: float
    rank_weight: float
    weights: dict[str, float]

    def predict_probability(
        self,
        *,
        rank: int,
        device: str,
        popularity_bucket: str,
        presentation_type: str,
    ) -> float:
        score = self.intercept + (self.rank_weight * self._normalize_rank(rank))
        for feature_key in self._feature_keys(
            device=device,
            popularity_bucket=popularity_bucket,
            presentation_type=presentation_type,
        ):
            score += self.weights.get(feature_key, 0.0)
        return _sigmoid(score)

    def _feature_keys(
        self,
        *,
        device: str,
        popularity_bucket: str,
        presentation_type: str,
    ) -> list[str]:
        feature_keys: list[str] = []
        if device != self.device_baseline:
            feature_keys.append(_feature_key("device", device))
        if popularity_bucket != self.popularity_baseline:
            feature_keys.append(_feature_key("popularity_bucket", popularity_bucket))
        if presentation_type != self.presentation_baseline:
            feature_keys.append(_feature_key("presentation_type", presentation_type))
        return feature_keys

    def _normalize_rank(self, rank: int) -> float:
        if self.rank_std <= 0:
            return 0.0
        return (rank - self.rank_mean) / self.rank_std


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
    from opensearchpy.helpers import scan

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


def build_impression_samples(events: list[dict[str, Any]]) -> list[ImpressionSample]:
    clicked_keys = {
        key
        for event in events
        if event.get("event_type") == "click"
        for key in [_event_join_key(event)]
        if key is not None
    }

    samples: list[ImpressionSample] = []
    for event in events:
        if event.get("event_type") != "impression":
            continue

        sample = _build_impression_sample(
            event,
            clicked=1 if _event_join_key(event) in clicked_keys else 0,
        )
        if sample is not None:
            samples.append(sample)
    return samples


def build_group_ctr_rows(
    samples: list[ImpressionSample],
    *,
    field_name: str,
    model: LogisticClickModel | None = None,
) -> list[dict[str, Any]]:
    if not samples:
        return []

    if model is None:
        model = fit_logistic_click_model(samples)
    rank_weights = _build_rank_weights(samples)
    per_group_rank_counts = _build_group_rank_counts(samples, field_name=field_name)

    rows = []
    for group in _sorted_group_values(samples, field_name):
        impressions = 0
        clicks = 0
        for sample in samples:
            if getattr(sample, field_name) != group:
                continue
            impressions += 1
            clicks += sample.clicked

        standardized_ctr, weight_coverage = _rank_standardized_ctr(
            per_group_rank_counts.get(group, {}),
            rank_weights,
        )
        adjusted_ctr = _regression_adjusted_group_ctr(
            samples,
            model,
            field_name=field_name,
            group=group,
        )
        rows.append(
            {
                "group": group,
                "impressions": impressions,
                "clicks": clicks,
                "raw_ctr": safe_ratio(clicks, impressions),
                "rank_standardized_ctr": standardized_ctr,
                "regression_adjusted_ctr": adjusted_ctr,
                "rank_weight_coverage": round(weight_coverage, 6),
            }
        )
    return rows


def build_rank_ctr_rows(
    samples: list[ImpressionSample],
    *,
    model: LogisticClickModel | None = None,
) -> list[dict[str, Any]]:
    if not samples:
        return []

    impressions: dict[int, int] = {}
    clicks: dict[int, int] = {}
    if model is None:
        model = fit_logistic_click_model(samples)

    for sample in samples:
        impressions[sample.rank] = impressions.get(sample.rank, 0) + 1
        clicks[sample.rank] = clicks.get(sample.rank, 0) + sample.clicked

    rows = []
    for rank in sorted(impressions):
        impression_count = impressions.get(rank, 0)
        click_count = clicks.get(rank, 0)
        adjusted_ctr = _regression_adjusted_rank_ctr(samples, model, rank=rank)
        rows.append(
            {
                "rank": rank,
                "impressions": impression_count,
                "clicks": click_count,
                "raw_ctr": safe_ratio(click_count, impression_count),
                "regression_adjusted_ctr": adjusted_ctr,
            }
        )
    return rows


def fit_logistic_click_model(samples: list[ImpressionSample]) -> LogisticClickModel:
    if not samples:
        return LogisticClickModel(
            rank_values=(1,),
            rank_mean=1.0,
            rank_std=1.0,
            device_values=("unknown",),
            popularity_values=("unknown",),
            presentation_values=("unknown",),
            device_baseline="unknown",
            popularity_baseline="unknown",
            presentation_baseline="unknown",
            intercept=0.0,
            rank_weight=0.0,
            weights={},
        )

    rank_values = tuple(sorted({sample.rank for sample in samples}))
    rank_mean = sum(sample.rank for sample in samples) / len(samples)
    rank_variance = sum((sample.rank - rank_mean) ** 2 for sample in samples) / len(samples)
    rank_std = math.sqrt(rank_variance) if rank_variance > 0 else 1.0
    device_values = tuple(sorted({sample.device for sample in samples}))
    popularity_values = tuple(
        sorted(
            {sample.popularity_bucket for sample in samples},
            key=lambda value: {"low": 0, "medium": 1, "high": 2}.get(value, 99),
        )
    )
    presentation_values = tuple(sorted({sample.presentation_type for sample in samples}))

    model = LogisticClickModel(
        rank_values=rank_values,
        rank_mean=rank_mean,
        rank_std=rank_std,
        device_values=device_values,
        popularity_values=popularity_values,
        presentation_values=presentation_values,
        device_baseline=device_values[0],
        popularity_baseline=popularity_values[0],
        presentation_baseline=presentation_values[0],
        intercept=0.0,
        rank_weight=0.0,
        weights={},
    )

    feature_space = _feature_space(model)
    weights = {feature_key: 0.0 for feature_key in feature_space}
    encoded_samples = [
        (
            model._normalize_rank(sample.rank),
            model._feature_keys(
                device=sample.device,
                popularity_bucket=sample.popularity_bucket,
                presentation_type=sample.presentation_type,
            ),
            sample.clicked,
        )
        for sample in samples
    ]

    intercept = 0.0
    rank_weight = 0.0
    learning_rate = 0.12
    l2_penalty = 0.001
    sample_count = len(encoded_samples)

    for _ in range(800):
        intercept_gradient = 0.0
        rank_gradient = 0.0
        gradients = {feature_key: 0.0 for feature_key in feature_space}

        for normalized_rank, active_features, clicked in encoded_samples:
            linear_score = intercept + (rank_weight * normalized_rank)
            for feature_key in active_features:
                linear_score += weights.get(feature_key, 0.0)

            probability = _sigmoid(linear_score)
            error = probability - clicked
            intercept_gradient += error
            rank_gradient += error * normalized_rank
            for feature_key in active_features:
                gradients[feature_key] += error

        intercept -= learning_rate * (intercept_gradient / sample_count)
        rank_weight -= learning_rate * ((rank_gradient / sample_count) + (l2_penalty * rank_weight))
        for feature_key in feature_space:
            penalty = l2_penalty * weights[feature_key]
            weights[feature_key] -= learning_rate * ((gradients[feature_key] / sample_count) + penalty)

    return LogisticClickModel(
        rank_values=model.rank_values,
        rank_mean=model.rank_mean,
        rank_std=model.rank_std,
        device_values=model.device_values,
        popularity_values=model.popularity_values,
        presentation_values=model.presentation_values,
        device_baseline=model.device_baseline,
        popularity_baseline=model.popularity_baseline,
        presentation_baseline=model.presentation_baseline,
        intercept=intercept,
        rank_weight=rank_weight,
        weights=weights,
    )


def _build_impression_sample(
    event: dict[str, Any],
    *,
    clicked: int,
) -> ImpressionSample | None:
    rank = coerce_int(event.get("rank"))
    device = event.get("device")
    popularity_bucket = event.get("popularity_bucket")
    presentation_type = event.get("presentation_type")

    if rank is None:
        return None
    if not isinstance(device, str) or not device:
        return None
    if not isinstance(popularity_bucket, str) or not popularity_bucket:
        return None
    if not isinstance(presentation_type, str) or not presentation_type:
        return None

    return ImpressionSample(
        rank=rank,
        device=device,
        popularity_bucket=popularity_bucket,
        presentation_type=presentation_type,
        clicked=clicked,
    )


def _event_join_key(event: dict[str, Any]) -> tuple[str, str, str, int] | None:
    session_id = event.get("session_id")
    query = event.get("query")
    item_id = event.get("item_id")
    rank = coerce_int(event.get("rank"))

    if not isinstance(session_id, str) or not session_id:
        return None
    if not isinstance(query, str) or not query:
        return None
    if not isinstance(item_id, str) or not item_id:
        return None
    if rank is None:
        return None
    return (session_id, query, item_id, rank)


def _build_group_rank_counts(
    samples: list[ImpressionSample],
    *,
    field_name: str,
) -> dict[str, dict[int, dict[str, int]]]:
    counts: dict[str, dict[int, dict[str, int]]] = {}
    for sample in samples:
        group = getattr(sample, field_name)
        group_counts = counts.setdefault(group, {})
        rank_counts = group_counts.setdefault(sample.rank, {"impressions": 0, "clicks": 0})
        rank_counts["impressions"] += 1
        rank_counts["clicks"] += sample.clicked
    return counts


def _build_rank_weights(samples: list[ImpressionSample]) -> dict[int, float]:
    total_impressions = len(samples)
    rank_counts: dict[int, int] = {}
    for sample in samples:
        rank_counts[sample.rank] = rank_counts.get(sample.rank, 0) + 1
    return {
        rank: count / total_impressions
        for rank, count in rank_counts.items()
    }


def _sorted_group_values(samples: list[ImpressionSample], field_name: str) -> list[str]:
    values = {getattr(sample, field_name) for sample in samples}
    if field_name == "popularity_bucket":
        ordered = sorted(values, key=lambda value: {"low": 0, "medium": 1, "high": 2}.get(value, 99))
        return list(ordered)
    return sorted(values)


def _rank_standardized_ctr(
    group_rank_counts: dict[int, dict[str, int]],
    rank_weights: dict[int, float],
) -> tuple[float, float]:
    weighted_sum = 0.0
    covered_weight = 0.0

    for rank, weight in rank_weights.items():
        rank_counts = group_rank_counts.get(rank)
        if not rank_counts or rank_counts["impressions"] <= 0:
            continue

        rank_ctr = rank_counts["clicks"] / rank_counts["impressions"]
        weighted_sum += weight * rank_ctr
        covered_weight += weight

    if covered_weight <= 0:
        return 0.0, 0.0
    return round(weighted_sum / covered_weight, 6), covered_weight


def _regression_adjusted_rank_ctr(
    samples: list[ImpressionSample],
    model: LogisticClickModel,
    *,
    rank: int,
) -> float:
    predicted_sum = 0.0
    for sample in samples:
        predicted_sum += model.predict_probability(
            rank=rank,
            device=sample.device,
            popularity_bucket=sample.popularity_bucket,
            presentation_type=sample.presentation_type,
        )
    return safe_ratio(predicted_sum, len(samples))


def _regression_adjusted_group_ctr(
    samples: list[ImpressionSample],
    model: LogisticClickModel,
    *,
    field_name: str,
    group: str,
) -> float:
    predicted_sum = 0.0
    for sample in samples:
        rank = sample.rank
        device = sample.device
        popularity_bucket = sample.popularity_bucket
        presentation_type = sample.presentation_type

        if field_name == "popularity_bucket":
            popularity_bucket = group
        elif field_name == "presentation_type":
            presentation_type = group
        else:
            raise ValueError(f"unsupported group field for regression adjustment: {field_name}")

        predicted_sum += model.predict_probability(
            rank=rank,
            device=device,
            popularity_bucket=popularity_bucket,
            presentation_type=presentation_type,
        )

    return safe_ratio(predicted_sum, len(samples))


def _feature_space(model: LogisticClickModel) -> list[str]:
    feature_keys: list[str] = []
    feature_keys.extend(
        _feature_key("device", device)
        for device in model.device_values
        if device != model.device_baseline
    )
    feature_keys.extend(
        _feature_key("popularity_bucket", popularity_bucket)
        for popularity_bucket in model.popularity_values
        if popularity_bucket != model.popularity_baseline
    )
    feature_keys.extend(
        _feature_key("presentation_type", presentation_type)
        for presentation_type in model.presentation_values
        if presentation_type != model.presentation_baseline
    )
    return feature_keys


def _feature_key(name: str, value: str | int) -> str:
    return f"{name}={value}"


def _sigmoid(value: float) -> float:
    clamped = max(min(value, 35.0), -35.0)
    return 1.0 / (1.0 + math.exp(-clamped))
