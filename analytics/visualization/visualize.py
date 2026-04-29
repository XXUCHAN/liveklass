from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt


REQUIRED_AGGREGATION_FILES = (
    "event_type_counts.json",
    "error_event_ratio.json",
    "rank_ctr.json",
    "popularity_ctr.json",
    "presentation_ctr.json",
)


@dataclass(frozen=True)
class VisualizationSettings:
    aggregations_dir: Path
    charts_dir: Path

    @classmethod
    def from_env(cls) -> "VisualizationSettings":
        return cls(
            aggregations_dir=Path(os.getenv("AGGREGATION_OUTPUT_DIR", "output/aggregations")),
            charts_dir=Path(os.getenv("CHART_OUTPUT_DIR", "output/charts")),
        )


def _require_aggregation_files(settings: VisualizationSettings) -> list[Path]:
    existing_files = [settings.aggregations_dir / filename for filename in REQUIRED_AGGREGATION_FILES]
    missing_files = [
        file_path.name for file_path in existing_files if not file_path.exists()
    ]
    if missing_files:
        raise RuntimeError(f"aggregation outputs not found: {', '.join(missing_files)}")
    return existing_files


def _clear_chart_directory(charts_dir: Path) -> None:
    charts_dir.mkdir(parents=True, exist_ok=True)
    for output_file in charts_dir.glob("*.png"):
        output_file.unlink()


def _read_json(file_path: Path) -> dict[str, Any]:
    return json.loads(file_path.read_text(encoding="utf-8"))


def _save_figure(output_path: Path) -> None:
    plt.tight_layout()
    plt.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close()


def _plot_event_type_counts(payload: dict[str, Any], output_path: Path) -> None:
    counts = payload.get("counts", {})
    labels = list(counts.keys())
    values = list(counts.values())

    plt.figure(figsize=(8, 4.8))
    plt.bar(labels, values, color="#2E86AB")
    plt.title("Event Type Counts")
    plt.xlabel("Event Type")
    plt.ylabel("Count")
    _save_figure(output_path)


def _plot_error_event_ratio(payload: dict[str, Any], output_path: Path) -> None:
    error_count = int(payload.get("error_event_count", 0))
    total_count = int(payload.get("total_event_count", 0))
    non_error_count = max(total_count - error_count, 0)

    plt.figure(figsize=(6.5, 4.5))
    plt.bar(["non_error", "error"], [non_error_count, error_count], color=["#6FB07F", "#D1495B"])
    ratio = float(payload.get("error_event_ratio", 0.0)) * 100
    plt.title(f"Error Event Ratio ({ratio:.2f}%)")
    plt.ylabel("Count")
    _save_figure(output_path)


def _plot_rank_ctr(payload: dict[str, Any], output_path: Path) -> None:
    rows = payload.get("rows", [])
    ranks = [int(row["rank"]) for row in rows]
    raw_ctr_values = [float(row["raw_ctr"]) * 100 for row in rows]
    adjusted_ctr_values = [float(row["regression_adjusted_ctr"]) * 100 for row in rows]

    plt.figure(figsize=(8, 4.8))
    plt.plot(ranks, raw_ctr_values, marker="o", color="#F18F01", label="Raw CTR")
    plt.plot(ranks, adjusted_ctr_values, marker="o", color="#2E86AB", label="Regression-adjusted CTR")
    plt.title("Raw vs Regression-adjusted CTR by Rank")
    plt.xlabel("Rank")
    plt.ylabel("CTR (%)")
    plt.xticks(ranks)
    plt.ylim(bottom=0)
    plt.legend()
    _save_figure(output_path)


def _plot_group_ctr(
    payload: dict[str, Any],
    output_path: Path,
    *,
    title: str,
) -> None:
    rows = payload.get("rows", [])
    groups = [str(row["group"]) for row in rows]
    raw_ctr_values = [float(row["raw_ctr"]) * 100 for row in rows]
    standardized_ctr_values = [float(row["rank_standardized_ctr"]) * 100 for row in rows]
    adjusted_ctr_values = [float(row["regression_adjusted_ctr"]) * 100 for row in rows]
    positions = list(range(len(groups)))
    bar_width = 0.24

    plt.figure(figsize=(8, 4.8))
    plt.bar(
        [position - bar_width for position in positions],
        raw_ctr_values,
        width=bar_width,
        color="#F18F01",
        label="Raw CTR",
    )
    plt.bar(
        positions,
        standardized_ctr_values,
        width=bar_width,
        color="#7FB069",
        label="Rank-standardized CTR",
    )
    plt.bar(
        [position + bar_width for position in positions],
        adjusted_ctr_values,
        width=bar_width,
        color="#2E86AB",
        label="Regression-adjusted CTR",
    )
    plt.title(title)
    plt.xlabel("Group")
    plt.ylabel("CTR (%)")
    plt.xticks(positions, groups)
    plt.ylim(bottom=0)
    plt.legend()
    _save_figure(output_path)


def main() -> None:
    settings = VisualizationSettings.from_env()
    settings.aggregations_dir.mkdir(parents=True, exist_ok=True)
    _clear_chart_directory(settings.charts_dir)

    _require_aggregation_files(settings)

    event_type_counts = _read_json(settings.aggregations_dir / "event_type_counts.json")
    error_event_ratio = _read_json(settings.aggregations_dir / "error_event_ratio.json")
    rank_ctr = _read_json(settings.aggregations_dir / "rank_ctr.json")
    popularity_ctr = _read_json(settings.aggregations_dir / "popularity_ctr.json")
    presentation_ctr = _read_json(settings.aggregations_dir / "presentation_ctr.json")

    _plot_event_type_counts(event_type_counts, settings.charts_dir / "event_type_counts.png")
    _plot_error_event_ratio(error_event_ratio, settings.charts_dir / "error_event_ratio.png")
    _plot_rank_ctr(rank_ctr, settings.charts_dir / "rank_ctr.png")
    _plot_group_ctr(
        popularity_ctr,
        settings.charts_dir / "popularity_ctr.png",
        title="Raw / Rank-standardized / Regression-adjusted CTR by Popularity Bucket",
    )
    _plot_group_ctr(
        presentation_ctr,
        settings.charts_dir / "presentation_ctr.png",
        title="Raw / Rank-standardized / Regression-adjusted CTR by Presentation Type",
    )

    print(
        json.dumps(
            {
                "status": "completed",
                "chart_files": sorted(
                    file_path.name for file_path in settings.charts_dir.glob("*.png")
                ),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
