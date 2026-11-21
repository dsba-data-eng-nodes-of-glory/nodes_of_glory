"""One-file customer onboarding: Spark cleanup, then existing model-facing tables.

Nothing in this module creates a user-facing workbook/template. Source columns
are mapped by the attached Spark pipeline; weather locations come from the form.
"""
from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from src.ingestion.workbook_loader import parse_selected_models
from src.ingestion.station_weather import workbook_weather_config

SOURCE_COLUMNS = {
    "demand": ["utc_timestamp", "demand"],
    "solar": ["utc_timestamp", "Photovoltaics [MWh]", "Photovoltaics_installed_capacity [MW]"],
    "wind": ["utc_timestamp", "Wind onshore [MWh]", "Wind_onshore_installed_capacity [MW]"],
}
COMPONENT_TO_KIND = {
    "demand_forecast": "demand",
    "solar_forecast": "solar",
    "wind_forecast": "wind",
    "residual_anomaly": "residual",
}


def parse_weather_locations(text: str) -> dict:
    """Read one service-area representative point per line: Name, latitude, longitude."""
    rows = []
    for i, line in enumerate((text or "").splitlines(), 1):
        if not line.strip():
            continue
        try:
            # Supports names containing a comma, e.g. "Cologne, Germany, 50.94, 6.96".
            name, latitude, longitude = [value.strip() for value in line.rsplit(",", 2)]
        except ValueError as exc:
            raise ValueError(
                f"Weather location line {i}: use Name, latitude, longitude."
            ) from exc
        rows.append({"location": name, "latitude": latitude, "longitude": longitude})
    if not rows:
        raise ValueError(
            "Enter at least one weather location (Name, latitude, longitude) in Station Management."
        )
    return workbook_weather_config(pd.DataFrame(rows))


def parse_column_overrides(text: str) -> dict[str, str]:
    """Optional corrections when a raw source uses unfamiliar headers.

    Example: demand=Net Load; wind_mw=Installed Wind MW
    """
    allowed = {"timestamp", "demand", "solar_mwh", "solar_mw", "wind_mwh", "wind_mw"}
    output = {}
    for i, line in enumerate((text or "").splitlines(), 1):
        if not line.strip():
            continue
        if "=" not in line:
            raise ValueError(f"Header mapping line {i}: expected canonical_key=Source Header")
        key, value = (part.strip() for part in line.split("=", 1))
        if key not in allowed or not value:
            raise ValueError(f"Header mapping line {i}: invalid key or missing header: {key}")
        if key in output:
            raise ValueError(f"Duplicate column mapping: {key}")
        output[key] = value
    return output


def load_cleaned_station_csv(
    cleaned_csv: Path,
    selected_models: list[str],
    weather_config: dict,
    interval_minutes: int,
) -> tuple[dict[str, pd.DataFrame], dict]:
    """Build exact canonical training/replay DataFrames without an intermediate XLSX."""
    if interval_minutes != 15:
        raise ValueError(
            "The supplied Spark source processor and renewable models require 15-minute records."
        )
    selected = parse_selected_models(selected_models)
    if "residual_anomaly" in selected and not {
        "demand_forecast", "solar_forecast", "wind_forecast"
    }.issubset(selected):
        raise ValueError("Residual Load requires Demand, Solar and Wind Energy models.")
    data = pd.read_csv(cleaned_csv, low_memory=False)
    required = sorted({column for kind in ("demand", "solar", "wind")
                       for column in SOURCE_COLUMNS[kind][1:]})
    absent = [col for col in ["timestamp", *required] if col not in data.columns]
    if absent:
        raise ValueError(f"Spark cleaned CSV has missing required fields: {absent}")
    timestamps = pd.to_datetime(data["timestamp"], format="%d-%m-%Y %H:%M",
                                utc=True, errors="coerce")
    if timestamps.isna().any():
        raise ValueError("Spark output contains an invalid UTC timestamp.")
    data["utc_timestamp"] = timestamps
    for col in required:
        data[col] = pd.to_numeric(data[col], errors="coerce")
        data.loc[~np.isfinite(data[col]), col] = np.nan
    data = data.sort_values("utc_timestamp").reset_index(drop=True)
    if data.utc_timestamp.duplicated().any():
        raise ValueError("Spark output contains duplicate UTC timestamps.")
    if not data.utc_timestamp.diff().dropna().eq(pd.Timedelta(minutes=15)).all():
        raise ValueError("Spark output contains missing or irregular 15-minute timestamps.")
    # Some leading source timestamps have no earlier observation to fill from.
    # Trim ONLY the unfillable edges, never silently remove interior intervals.
    good = data[required].notna().all(axis=1)
    if not good.any():
        raise ValueError("All cleaned rows still contain missing measurements.")
    first = int(np.flatnonzero(good.to_numpy())[0])
    last = int(np.flatnonzero(good.to_numpy())[-1])
    interior = data.iloc[first:last + 1]
    if interior[required].isna().any().any():
        missing = interior[required].isna().sum()
        raise ValueError("The Spark cleaner left unfilled interior measurements: "
                         + str(missing[missing > 0].to_dict()))
    trimmed = first + len(data) - last - 1
    data = interior.reset_index(drop=True)
    for col in required:
        if (data[col] < 0).any():
            raise ValueError(f"Negative measurements are not allowed in {col}.")
    for col in ("Photovoltaics_installed_capacity [MW]", "Wind_onshore_installed_capacity [MW]"):
        if (data[col] <= 0).any():
            raise ValueError(f"Installed capacity must be positive for every row: {col}")

    required_kinds = {COMPONENT_TO_KIND[model] for model in selected}
    if "residual" in required_kinds:
        required_kinds.remove("residual")
        required_kinds.update(("demand", "solar", "wind"))
    output = {kind: data[SOURCE_COLUMNS[kind]].copy()
              for kind in sorted(required_kinds)}
    diagnostics = {
        "selected_models": selected,
        "interval_minutes": interval_minutes,
        "source_timezone": "UTC",
        "weather": weather_config,
        "raw_source_processed_by": "spark_rdd_preprocessing",
        "raw_unfillable_edge_rows_trimmed": trimmed,
        "worksheets": {
            kind.title(): {
                "rows": int(len(frame)),
                "start_utc": frame.utc_timestamp.iloc[0].isoformat(),
                "end_utc": frame.utc_timestamp.iloc[-1].isoformat(),
                "columns": list(frame.columns),
            } for kind, frame in output.items()
        },
    }
    return output, diagnostics


def spark_clean_raw_source(
    source_path: Path, staging_dir: Path, project_root: Path, station_id: str,
    *, source_timezone: str = "auto", interval_minutes: int = 15,
    number_locale: str = "de", sheet_name: str = "",
    column_overrides: str = "",
) -> tuple[Path, dict, Path]:
    """Run attached Spark cleaner in its own JVM, independent of live streaming queries."""
    script = project_root / "scripts" / "raw_spark_preprocessor.py"
    if not script.is_file():
        raise FileNotFoundError(f"Raw Spark preprocessor not installed at {script}")
    if interval_minutes != 15:
        raise ValueError("Raw onboarding currently supports 15-minute records.")
    if source_timezone == "":
        source_timezone = "auto"
    mapping = parse_column_overrides(column_overrides)
    cmd = [
        sys.executable, str(script), str(source_path),
        "--source-timezone", source_timezone, "--interval-minutes", str(interval_minutes),
        "--number-locale", number_locale,
        "--column-map-json", json.dumps(mapping),
        "--causal-only",
    ]
    if sheet_name.strip():
        cmd += ["--sheet-name", sheet_name.strip()]
    # Keep the complete log outside the temporary staging folder on failure.
    log_path = project_root / "logs" / f"{station_id}_raw_spark.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    spark_yaml = project_root / "config" / "spark.yaml"
    if spark_yaml.exists():
        import yaml
        spark_cfg = yaml.safe_load(spark_yaml.read_text(encoding="utf-8")) or {}
        if spark_cfg.get("java_home"):
            env["JAVA_HOME"] = str(spark_cfg["java_home"])
    env["PYSPARK_PYTHON"] = sys.executable
    env["PYSPARK_DRIVER_PYTHON"] = sys.executable
    try:
        with log_path.open("w", encoding="utf-8") as logfile:
            completed = subprocess.run(
                cmd, cwd=project_root, env=env, stdout=logfile,
                stderr=subprocess.STDOUT, timeout=2400, check=False,
            )
    except subprocess.TimeoutExpired as exc:
        raise ValueError(
            f"Spark preprocessing exceeded its 40-minute limit. Check {log_path}."
        ) from exc
    if completed.returncode:
        detail = log_path.read_text(encoding="utf-8", errors="replace")[-3500:]
        raise ValueError(
            "Spark preprocessing rejected or could not process the raw upload. "
            f"See logs/{station_id}_raw_spark.log. Details:\n{detail}"
        )
    cleaned = source_path.with_name(source_path.stem + "_cleaned.csv")
    quality = source_path.with_name(source_path.stem + "_quality.json")
    if not cleaned.is_file() or not quality.is_file():
        raise ValueError("Spark completed without creating both cleaned CSV and quality JSON.")
    report = json.loads(quality.read_text(encoding="utf-8"))
    if not report.get("data_accepted"):
        raise ValueError("Spark quality gate did not accept the source.")
    return cleaned, report, log_path
