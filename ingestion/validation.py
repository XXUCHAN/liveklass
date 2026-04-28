from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

ALLOWED_EVENT_TYPES = {"page_view", "impression", "click", "purchase", "error"}
ALLOWED_DEVICES = {"mobile", "desktop", "tablet"}
ALLOWED_POPULARITY_BUCKETS = {"low", "medium", "high"}
ALLOWED_PRESENTATION_TYPES = {
    "normal_card",
    "featured_card",
    "discount_badge",
    "live_badge",
}


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    normalized_event: dict[str, Any] | None = None
    errors: list[str] = field(default_factory=list)


def _is_non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


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


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _validate_common_fields(
    event: dict[str, Any],
    errors: list[str],
    *,
    late_arrival_threshold_seconds: float,
) -> dict[str, Any]:
    normalized = dict(event)
    ingested_at = datetime.now(UTC)
    normalized["ingested_at"] = _format_timestamp(ingested_at)

    for required_field in ("event_id", "schema_version", "user_id", "session_id"):
        if not _is_non_empty_string(normalized.get(required_field)):
            errors.append(f"{required_field} is required")

    event_type = normalized.get("event_type")
    if not _is_non_empty_string(event_type):
        errors.append("event_type is required")
    elif event_type not in ALLOWED_EVENT_TYPES:
        errors.append(f"event_type must be one of {sorted(ALLOWED_EVENT_TYPES)}")

    event_time = _parse_timestamp(normalized.get("event_time"))
    if event_time is None:
        errors.append("event_time must be a parseable timestamp")
    else:
        normalized["event_time"] = _format_timestamp(event_time)

    received_at_value = normalized.get("received_at")
    if received_at_value is None:
        normalized["received_at"] = _format_timestamp(ingested_at)
    else:
        received_at = _parse_timestamp(received_at_value)
        if received_at is None:
            errors.append("received_at must be a parseable timestamp when provided")
        else:
            normalized["received_at"] = _format_timestamp(received_at)

    if event_time is not None:
        arrival_lag_seconds = max(0.0, (ingested_at - event_time).total_seconds())
        normalized["arrival_lag_seconds"] = round(arrival_lag_seconds, 3)
        normalized["is_late_arrival"] = arrival_lag_seconds > late_arrival_threshold_seconds

    device = normalized.get("device")
    if not _is_non_empty_string(device):
        errors.append("device is required")
    elif device not in ALLOWED_DEVICES:
        errors.append(f"device must be one of {sorted(ALLOWED_DEVICES)}")

    return normalized


def _validate_page_view(event: dict[str, Any], errors: list[str]) -> None:
    if not _is_non_empty_string(event.get("page_url")):
        errors.append("page_url is required for page_view event")


def _validate_impression(event: dict[str, Any], errors: list[str]) -> None:
    if not _is_non_empty_string(event.get("query")):
        errors.append("query is required for impression event")
    if not _is_non_empty_string(event.get("item_id")):
        errors.append("item_id is required for impression event")

    rank = _coerce_int(event.get("rank"))
    if rank is None:
        errors.append("rank is required for impression event")
    elif not 1 <= rank <= 10:
        errors.append("rank must be between 1 and 10 for impression event")

    popularity_bucket = event.get("popularity_bucket")
    if popularity_bucket not in ALLOWED_POPULARITY_BUCKETS:
        errors.append(
            f"popularity_bucket must be one of {sorted(ALLOWED_POPULARITY_BUCKETS)}"
        )

    presentation_type = event.get("presentation_type")
    if presentation_type not in ALLOWED_PRESENTATION_TYPES:
        errors.append(
            f"presentation_type must be one of {sorted(ALLOWED_PRESENTATION_TYPES)}"
        )

    position_bias = event.get("position_bias")
    if position_bias is not None:
        position_bias_value = _coerce_float(position_bias)
        if position_bias_value is None:
            errors.append("position_bias must be numeric when provided")


def _validate_click(event: dict[str, Any], errors: list[str]) -> None:
    if not _is_non_empty_string(event.get("query")):
        errors.append("query is required for click event")
    if not _is_non_empty_string(event.get("item_id")):
        errors.append("item_id is required for click event")

    rank = _coerce_int(event.get("rank"))
    if rank is None:
        errors.append("rank is required for click event")
    elif not 1 <= rank <= 10:
        errors.append("rank must be between 1 and 10 for click event")

    click_prob = _coerce_float(event.get("click_prob"))
    if click_prob is None:
        errors.append("click_prob is required for click event")
    elif not 0.0 <= click_prob <= 1.0:
        errors.append("click_prob must be between 0 and 1 for click event")

    popularity_bucket = event.get("popularity_bucket")
    if popularity_bucket not in ALLOWED_POPULARITY_BUCKETS:
        errors.append(f"popularity_bucket must be one of {sorted(ALLOWED_POPULARITY_BUCKETS)}")

    presentation_type = event.get("presentation_type")
    if presentation_type not in ALLOWED_PRESENTATION_TYPES:
        errors.append(
            f"presentation_type must be one of {sorted(ALLOWED_PRESENTATION_TYPES)}"
        )

    position_bias = event.get("position_bias")
    if position_bias is not None:
        position_bias_value = _coerce_float(position_bias)
        if position_bias_value is None:
            errors.append("position_bias must be numeric when provided")


def _validate_purchase(event: dict[str, Any], errors: list[str]) -> None:
    if not _is_non_empty_string(event.get("item_id")):
        errors.append("item_id is required for purchase event")

    amount = _coerce_float(event.get("amount"))
    if amount is None:
        errors.append("amount is required for purchase event")
    elif amount < 0:
        errors.append("amount must be greater than or equal to 0 for purchase event")


def _validate_error(event: dict[str, Any], errors: list[str]) -> None:
    if not _is_non_empty_string(event.get("error_code")):
        errors.append("error_code is required for error event")
    if not _is_non_empty_string(event.get("error_message")):
        errors.append("error_message is required for error event")


def validate_event(
    event: Any,
    *,
    late_arrival_threshold_seconds: float = 3600.0,
) -> ValidationResult:
    if not isinstance(event, dict):
        return ValidationResult(is_valid=False, errors=["event payload must be an object"])

    errors: list[str] = []
    normalized = _validate_common_fields(
        event,
        errors,
        late_arrival_threshold_seconds=late_arrival_threshold_seconds,
    )
    event_type = normalized.get("event_type")

    if event_type == "page_view":
        _validate_page_view(normalized, errors)
    elif event_type == "impression":
        _validate_impression(normalized, errors)
    elif event_type == "click":
        _validate_click(normalized, errors)
    elif event_type == "purchase":
        _validate_purchase(normalized, errors)
    elif event_type == "error":
        _validate_error(normalized, errors)

    if errors:
        return ValidationResult(is_valid=False, errors=errors)

    return ValidationResult(is_valid=True, normalized_event=normalized)
