"""Customer-facing, conservative summary of Spark's raw-file quality audit.

Accepted by Spark does not mean the targets are reliable for forecasting.
A hard Spark rejection cannot be overridden by this confirmation workflow.
"""
from __future__ import annotations

from typing import Any

_LABELS = {
    "demand": "Demand",
    "solar_mwh": "Solar generation",
    "wind_mwh": "Wind Energy generation",
    "solar_mw": "Solar installed capacity",
    "wind_mw": "Wind Energy installed capacity",
}


def _nonzero_map(values: Any) -> dict[str, int]:
    if not isinstance(values, dict):
        return {}
    return {str(_LABELS.get(str(k), str(k))): int(v)
            for k, v in values.items() if isinstance(v, (int, float)) and v > 0}


def build_quality_review(report: dict, station: str) -> dict:
    if not report or not report.get("data_accepted"):
        raise ValueError("Only Spark-accepted datasets can be submitted for customer review")
    issues: list[dict] = []

    def add(level: str, title: str, detail: str):
        issues.append({"severity": level, "title": title, "detail": detail})

    missing_slots = int(report.get("missing_timestamp_slots") or 0)
    timestamp_pct = float(report.get("missing_timestamp_pct") or 0)
    if missing_slots:
        add("warning", "Missing timestamps repaired",
            f"{missing_slots:,} missing intervals ({timestamp_pct:.2f}%). Confirm the historical baseline is suitable.")
    missing_targets = report.get("target_missing_pct_before_fill") or {}
    if isinstance(missing_targets, dict):
        for metric, pct in missing_targets.items():
            try:
                pct = float(pct)
            except (TypeError, ValueError):
                continue
            if pct > 0:
                add("warning", f"{_LABELS.get(metric, metric)} has missing source readings",
                    f"{pct:.2f}% were missing/invalid before imputation; evaluation must exclude imputed targets.")
    duplicates = int(report.get("duplicate_rows_removed") or report.get("duplicate_rows_reconciled") or 0)
    if duplicates:
        add("info", "Duplicate timestamps reconciled", f"{duplicates:,} duplicate rows were reconciled.")
    imputed = _nonzero_map(report.get("imputed_cell_counts"))
    if imputed:
        add("warning", "Historical values imputed",
            ", ".join(f"{name}: {count:,}" for name, count in imputed.items()))
    unfilled = _nonzero_map(report.get("unfilled_cell_counts"))
    if unfilled:
        add("high", "Unfilled measurements remain",
            ", ".join(f"{name}: {count:,}" for name, count in unfilled.items()))
    malformed = int(report.get("source_malformed_cells") or 0)
    if malformed:
        add("warning", "Malformed source measurements", f"{malformed:,} cells were invalid or malformed.")
    for key, label in (("solar_generation_exceeds_reported_capacity_intervals", "Solar"),
                       ("wind_generation_exceeds_reported_capacity_intervals", "Wind Energy")):
        n = int(report.get(key) or 0)
        if n:
            add("high", f"{label} generation exceeds installed capacity",
                f"{n:,} intervals exceeded the configured capacity tolerance. Verify measurement units and the capacity series before using the trained model.")
    flatlines = int(report.get("observed_demand_flatline_runs_24h") or 0)
    if flatlines:
        add("high", "Unusually long constant Demand", f"{flatlines:,} flatline runs exceeded 24 hours.")
    if report.get("demand_unit_unverified"):
        add("warning", "Demand unit not verified", "Confirm whether Demand is MW or MWh per interval.")
    for warning in (report.get("warnings") or []):
        warning = str(warning)
        if warning and not any(warning.lower() in str(i["detail"]).lower() for i in issues):
            add("warning", "Spark preprocessing warning", warning)
    risk = bool(issues) or not bool(report.get("training_ready", False))
    if not report.get("training_ready", False) and not issues:
        add("high", "Training readiness not established", "Spark accepted the dataset, but marked it not training-ready.")

    return {
        "station": station,
        "data_accepted": True,
        "training_ready": bool(report.get("training_ready", False)),
        "training_risk": risk,
        "rows": int(report.get("rows") or 0),
        "missing_timestamp_pct": round(timestamp_pct, 3),
        "issues": issues,
        "warning_count": sum(i["severity"] in {"warning", "high"} for i in issues),
        "summary": ("Quality concerns may compromise forecast accuracy. Continue only after reviewing the input data."
                    if risk else "No quality warnings were found; customer approval is still required to proceed."),
        "approval_statement": (
            "I reviewed the data-quality findings and understand that training, validation, forecasts, "
            "and residual-load anomaly results may be less reliable if I proceed. "
            "I authorize registration, weather enrichment, training and deployment for this station."
        ),
    }
