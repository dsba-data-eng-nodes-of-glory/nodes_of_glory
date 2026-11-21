"""Regression checks for one-file raw station onboarding (no running Spark required)."""
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd
import pytest

from src.ingestion.raw_station_loader import (
    parse_column_overrides, parse_weather_locations, load_cleaned_station_csv,
)


def test_weather_location_form():
    config = parse_weather_locations("A, 51.5, 7.4\nB, 50.9, 6.9")
    assert len(config["sites"]) == 2
    assert config["provider"] == "open_meteo"


def test_weather_location_requires_coordinates():
    with pytest.raises(ValueError, match="latitude"):
        parse_weather_locations("A, not_lat, 6.5")


def test_optional_raw_source_header_overrides():
    assert parse_column_overrides("demand=Net Load\nwind_mw=Wind Installed") == {
        "demand": "Net Load", "wind_mw": "Wind Installed",
    }


def test_unknown_override_disallowed():
    with pytest.raises(ValueError):
        parse_column_overrides("bad=Whatever")


def test_cleaned_source_yields_models():
    with tempfile.TemporaryDirectory() as temporary:
        file = Path(temporary) / "cleaned.csv"
        n = 900
        ts = pd.date_range("2026-07-01T00:00:00Z", periods=n, freq="15min")
        frame = pd.DataFrame({
            "timestamp": ts.strftime("%d-%m-%Y %H:%M"),
            "demand": np.arange(n, dtype=float) + 1000,
            "Photovoltaics [MWh]": np.arange(n, dtype=float) / 10,
            "Photovoltaics_installed_capacity [MW]": np.full(n, 1500),
            "Wind onshore [MWh]": np.full(n, 100),
            "Wind_onshore_installed_capacity [MW]": np.full(n, 1500),
        })
        frame.to_csv(file, index=False)
        config = parse_weather_locations("A, 51.5, 7.4")
        models = ["demand_forecast", "solar_forecast", "wind_forecast", "residual_anomaly"]
        datasets, diagnostics = load_cleaned_station_csv(file, models, config, 15)
        assert set(datasets) == {"demand", "solar", "wind"}
        assert len(datasets["demand"]) == n
        assert list(datasets["wind"]) == [
            "utc_timestamp", "Wind onshore [MWh]", "Wind_onshore_installed_capacity [MW]"
        ]
        assert diagnostics["weather"]["sites"][0]["location"] == "A"


def test_missing_interior_measurement_rejected():
    with tempfile.TemporaryDirectory() as temporary:
        p = Path(temporary) / "clean.csv"
        ts = pd.date_range("2026-09-01T00:00:00Z", periods=5, freq="15min")
        frame = pd.DataFrame({
            "timestamp": ts.strftime("%d-%m-%Y %H:%M"),
            "demand": [1, 2, np.nan, 4, 5],
            "Photovoltaics [MWh]": [1] * 5,
            "Photovoltaics_installed_capacity [MW]": [10] * 5,
            "Wind onshore [MWh]": [1] * 5,
            "Wind_onshore_installed_capacity [MW]": [10] * 5,
        })
        frame.to_csv(p, index=False)
        with pytest.raises(ValueError, match="unfilled interior"):
            load_cleaned_station_csv(p, ["demand_forecast"], parse_weather_locations("A, 51, 7"), 15)


def test_residual_requires_all_targets():
    with pytest.raises(ValueError, match="Residual Load"):
        load_cleaned_station_csv(Path("no-file.csv"), ["residual_anomaly"],
                                 parse_weather_locations("A, 51, 7"), 15)
