"""The extra learning inputs are opt-in for Random Forest only."""
import json

import numpy as np
import pandas as pd
import pytest
import yaml
from sklearn.dummy import DummyRegressor

from src.models import model_registry, train_tabular
from src.models.tabular_forecasting import (
    DAY_ONLY_FEATURE_SCHEMA, LEGACY_FEATURE_SCHEMA, TARGET_DAY_FEATURE, TARGET_WEEK_FEATURE,
    build_training_examples,
)


@pytest.mark.parametrize("algorithm,expected_schema,feature_count", [
    ("random_forest", DAY_ONLY_FEATURE_SCHEMA, 37),
    ("dense_nn", LEGACY_FEATURE_SCHEMA, 36),
    ("gbt", LEGACY_FEATURE_SCHEMA, 36),
    ("xgboost", LEGACY_FEATURE_SCHEMA, 36),
])
def test_training_selects_new_features_only_for_random_forest(
    tmp_path, monkeypatch, algorithm, expected_schema, feature_count,
):
    timestamps = pd.date_range("2025-01-01", periods=2000, freq="15min", tz="UTC")
    frame = pd.DataFrame({
        "utc_timestamp": timestamps.astype("datetime64[ns, UTC]"),
        "demand": 1000 + np.arange(2000, dtype=float), "temperature": 10.0,
    })
    config = tmp_path / "config"
    config.mkdir()
    (config / "stations.yaml").write_text(yaml.safe_dump({
        "stations": {"demo": {"training_file": "source.csv", "interval_minutes": 15}}
    }))
    (config / "model.yaml").write_text("history_window_hours: 168\nforecast_horizon_hours: 24\n")
    frame.to_csv(tmp_path / "source.csv", index=False)
    monkeypatch.setattr(train_tabular, "ROOT", tmp_path)
    monkeypatch.setattr(train_tabular, "load_station_csv", lambda *args, **kwargs: frame.copy())
    monkeypatch.setattr(model_registry, "REGISTRY_PATH", tmp_path / "registry.json")
    # This tests input routing, not any estimator's implementation.
    inputs_seen = []

    class InputRecorder(DummyRegressor):
        def fit(self, x, y):
            inputs_seen.append(x.copy())
            return super().fit(x, y)

    monkeypatch.setattr(train_tabular, "build_estimator", lambda *args: InputRecorder())
    monkeypatch.setattr(train_tabular.joblib, "dump", lambda model, path, **kwargs: path.write_bytes(b"test"))
    experiment_id = f"scope-{algorithm}"
    metadata = train_tabular.run_training({
        "station": "demo", "algorithm": algorithm, "experiment_id": experiment_id,
        "parameters": {"max_train_origins": 3, "max_validation_origins": 2},
        "training_options": {"validation_fraction": 0.15},
    })
    assert metadata["feature_schema"] == expected_schema
    assert len(metadata["feature_names"]) == feature_count
    assert inputs_seen[0].shape == (288, feature_count)
    references, _, _ = build_training_examples(frame, np.array([672, 1138, 1604]), 672, 96, 15)
    # Shared/default callers still receive the exact original feature matrix.
    if algorithm != "random_forest":
        np.testing.assert_array_equal(inputs_seen[0], references.to_numpy(dtype=np.float32))
        assert TARGET_DAY_FEATURE not in metadata["feature_names"]
        assert TARGET_WEEK_FEATURE not in metadata["feature_names"]
    else:
        assert TARGET_DAY_FEATURE in metadata["feature_names"]
        assert TARGET_WEEK_FEATURE not in metadata["feature_names"]
        assert "demand_lag_672" in metadata["feature_names"]
    saved = json.loads((tmp_path / metadata["metadata_path"]).read_text())
    assert saved["feature_schema"] == expected_schema
    assert model_registry.get_experiment(experiment_id)["feature_schema"] == expected_schema


@pytest.mark.parametrize("algorithm", ["random_forest", "dense_nn"])
def test_weather_and_test_split_keep_their_existing_contract(tmp_path, monkeypatch, algorithm):
    """Exercise all three matrices and saved metadata, including NN blending."""
    from src.ingestion.station_weather import FORECAST_WEATHER_MODE

    frame = pd.DataFrame({
        "utc_timestamp": pd.date_range("2025-01-01", periods=3000, freq="15min", tz="UTC"),
        "demand": 1000 + np.arange(3000, dtype=float),
        "temperature": 10.0,
        "forecast_temperature": 12.0,
    })
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "stations.yaml").write_text(yaml.safe_dump({"stations": {"demo": {
        "training_file": "source.csv", "interval_minutes": 15,
        "weather": {"sites": [{"name": "demo", "latitude": 51.0, "longitude": 7.0}]},
        "replay_start_date": frame.utc_timestamp.iloc[2800].isoformat(),
    }}}))
    (config_dir / "model.yaml").write_text("history_window_hours: 168\nforecast_horizon_hours: 24\n")
    frame.to_csv(tmp_path / "source.csv", index=False)
    monkeypatch.setattr(train_tabular, "ROOT", tmp_path)
    monkeypatch.setattr(train_tabular, "load_station_csv", lambda *args, **kwargs: frame.copy())
    monkeypatch.setattr(model_registry, "REGISTRY_PATH", tmp_path / "registry.json")
    matrices = []

    class InputRecorder(DummyRegressor):
        def fit(self, x, y):
            matrices.append(x.copy())
            return super().fit(x, y)

        def predict(self, x):
            matrices.append(x.copy())
            return super().predict(x)

    monkeypatch.setattr(train_tabular, "build_estimator", lambda *args: InputRecorder())
    monkeypatch.setattr(train_tabular.joblib, "dump", lambda model, path, **kwargs: path.write_bytes(b"test"))
    metadata = train_tabular.run_training({
        "station": "demo", "algorithm": algorithm, "experiment_id": f"weather-{algorithm}",
        "parameters": {"max_train_origins": 3, "max_validation_origins": 2},
    })
    count = 37 if algorithm == "random_forest" else 36
    assert [matrix.shape for matrix in matrices] == [(288, count), (192, count), (192, count)]
    assert metadata["training_rows"] == 2800  # Replay observations stay outside all splits.
    assert metadata["weather_mode"] == FORECAST_WEATHER_MODE
    assert "future_temperature_forecast" in metadata["feature_names"]
    assert "future_temperature_persistence" not in metadata["feature_names"]
    assert metadata["evaluation_protocol"] == "chronological_train_validation_test_sampled_origins_v1"
    assert metadata["test_metrics"]["matched_observations"] == 192
    assert pd.Timestamp(metadata["training_end_utc"]) < pd.Timestamp(metadata["validation_start_utc"])
    assert pd.Timestamp(metadata["validation_end_utc"]) < pd.Timestamp(metadata["test_start_utc"])
    if algorithm == "dense_nn":
        assert "weekly_blend_model_weight" in metadata
        assert "unblended_test_metrics" in metadata
    else:
        assert "weekly_blend_model_weight" not in metadata
        day_index = metadata["feature_names"].index(TARGET_DAY_FEATURE)
        # Every split receives the target-aligned previous-day values.
        for matrix in matrices:
            assert np.isfinite(matrix[:, day_index]).all()
