from __future__ import annotations

POSITION_BIAS = {
    1: 0.95,
    2: 0.75,
    3: 0.60,
    4: 0.40,
    5: 0.25,
    6: 0.18,
    7: 0.12,
    8: 0.08,
    9: 0.05,
    10: 0.03,
}

POPULARITY_BOOST = {
    "low": 0.8,
    "medium": 1.0,
    "high": 1.3,
}

PRESENTATION_BOOST = {
    "normal_card": 1.0,
    "featured_card": 1.25,
    "discount_badge": 1.15,
    "live_badge": 1.2,
}


def clamp_probability(value: float, minimum: float = 0.0, maximum: float = 0.95) -> float:
    return max(minimum, min(value, maximum))


def get_position_bias(rank: int) -> float:
    if rank not in POSITION_BIAS:
        raise ValueError(f"rank must be between 1 and 10, got {rank}")
    return POSITION_BIAS[rank]


def calculate_click_probability(
    base_relevance: float,
    rank: int,
    popularity_bucket: str,
    presentation_type: str,
) -> float:
    if popularity_bucket not in POPULARITY_BOOST:
        raise ValueError(f"unsupported popularity_bucket: {popularity_bucket}")
    if presentation_type not in PRESENTATION_BOOST:
        raise ValueError(f"unsupported presentation_type: {presentation_type}")

    click_prob = (
        base_relevance
        * get_position_bias(rank)
        * POPULARITY_BOOST[popularity_bucket]
        * PRESENTATION_BOOST[presentation_type]
    )
    return round(clamp_probability(click_prob), 4)
