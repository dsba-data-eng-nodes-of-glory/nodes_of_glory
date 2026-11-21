from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sklearn.ensemble import RandomForestRegressor

from src.models.tabular_forecasting import (
    DAY_ONLY_FEATURE_SCHEMA, FEATURE_SCHEMA, LEGACY_FEATURE_SCHEMA, TARGET_DAY_FEATURE, TARGET_WEEK_FEATURE,
    build_origin_features, build_training_examples, target_demand_references,
)


def history_and_targets(interval=15):
    steps = 10080 // interval
    history = pd.DataFrame({
        "utc_timestamp": pd.date_range("2025-03-23T00:00:00Z", periods=steps,
                                       freq=f"{interval}min"),
        "demand": np.arange(steps, dtype=float) + 1000,
        "temperature": np.full(steps, 10.0),
    })
    history["utc_timestamp"] = history["utc_timestamp"].astype("datetime64[ns, UTC]")
    targets = pd.date_range(history["utc_timestamp"].iloc[-1] + pd.Timedelta(minutes=interval),
                            periods=1440 // interval, freq=f"{interval}min")
    return history, targets


@pytest.mark.parametrize("interval", [15, 30, 60, 90, 120])
def test_target_features_follow_each_target_timestamp(interval):
    history, targets = history_and_targets(interval)
    features = build_origin_features(history, targets, interval, feature_schema=FEATURE_SCHEMA)
    steps = len(targets)
    np.testing.assert_array_equal(features[TARGET_DAY_FEATURE], history["demand"].iloc[-steps:])
    np.testing.assert_array_equal(features[TARGET_WEEK_FEATURE], history["demand"].iloc[:steps])
    assert features[TARGET_DAY_FEATURE].iloc[-1] == history["demand"].iloc[-1]


def test_references_reject_horizon_requiring_future_observations():
    history, targets = history_and_targets()
    extended = targets.append(pd.DatetimeIndex([targets[-1] + pd.Timedelta(minutes=15)]))
    with pytest.raises(ValueError, match="reference is unavailable"):
        build_origin_features(history, extended, feature_schema=FEATURE_SCHEMA)


def test_references_do_not_substitute_nearby_timestamps():
    history, targets = history_and_targets()
    with pytest.raises(ValueError, match="reference is unavailable"):
        target_demand_references(history, targets + pd.Timedelta(minutes=1))


def test_legacy_feature_schema_preserves_existing_model_inputs():
    history, targets = history_and_targets()
    legacy = build_origin_features(history, targets, feature_schema=LEGACY_FEATURE_SCHEMA)
    updated = build_origin_features(history, targets, feature_schema=FEATURE_SCHEMA)
    pd.testing.assert_frame_equal(legacy, build_origin_features(history, targets))
    assert TARGET_DAY_FEATURE not in legacy and TARGET_WEEK_FEATURE not in legacy
    pd.testing.assert_frame_equal(legacy, updated[legacy.columns])
    with pytest.raises(ValueError, match="Unsupported"):
        build_origin_features(history, targets, feature_schema="unknown")


@pytest.mark.parametrize("schema", [FEATURE_SCHEMA, DAY_ONLY_FEATURE_SCHEMA])
def test_changing_future_labels_cannot_change_features(schema):
    history, targets = history_and_targets()
    future = pd.DataFrame({"utc_timestamp": targets, "demand": 2000.0, "temperature": 99.0})
    frame = pd.concat([history, future], ignore_index=True)
    changed = frame.copy()
    changed.loc[len(history):, ["demand", "temperature"]] = -123456.0
    x, y, _ = build_training_examples(frame, np.array([len(history)]), len(history), len(targets), 15, feature_schema=schema)
    other_x, other_y, _ = build_training_examples(changed, np.array([len(history)]), len(history), len(targets), 15, feature_schema=schema)
    pd.testing.assert_frame_equal(x, other_x)
    assert not np.array_equal(y, other_y)


@pytest.mark.parametrize("schema", [None, LEGACY_FEATURE_SCHEMA, FEATURE_SCHEMA, DAY_ONLY_FEATURE_SCHEMA])
def test_serving_uses_saved_schema_and_matches_training_predictions(schema):
    import torch
    from src.models.streaming_predictor import generate_forecast

    history, targets = history_and_targets()
    features = build_origin_features(history, targets, feature_schema=schema or LEGACY_FEATURE_SCHEMA)
    model = RandomForestRegressor(n_estimators=2, max_depth=2, random_state=42)
    model.fit(features.to_numpy(dtype=np.float32), np.arange(len(targets)))
    checkpoint = {
        "algorithm": "random_forest", "region_name": "demo", "feature_schema": schema,
        "feature_names": list(features), "history": len(history), "horizon": len(targets),
        "interval_minutes": 15,
    }
    if schema is None:
        checkpoint.pop("feature_schema")  # Original artifacts may omit the field.
    rows = generate_forecast(history.to_dict("records"), model, checkpoint, torch.device("cpu"), Path("model.joblib"))
    np.testing.assert_array_equal(
        [row["predicted_demand"] for row in rows],
        model.predict(features.to_numpy(dtype=np.float32)),
    )


@pytest.mark.parametrize("interval", [15, 30, 60, 90, 120])
def test_day_only_schema_matches_tested_ablation_and_preserves_original_lags(interval):
    history, targets = history_and_targets(interval)
    both = build_origin_features(history, targets, interval, feature_schema=FEATURE_SCHEMA)
    day_only = build_origin_features(history, targets, interval, feature_schema=DAY_ONLY_FEATURE_SCHEMA)
    legacy = build_origin_features(history, targets, interval, feature_schema=LEGACY_FEATURE_SCHEMA)
    pd.testing.assert_frame_equal(day_only, both.drop(columns=TARGET_WEEK_FEATURE))
    pd.testing.assert_frame_equal(day_only[list(legacy)], legacy)
    assert TARGET_DAY_FEATURE in day_only and TARGET_WEEK_FEATURE not in day_only
    assert f"demand_lag_{10080 // interval}" in day_only
    np.testing.assert_array_equal(day_only[TARGET_DAY_FEATURE], history["demand"].iloc[-len(targets):])
    if interval == 15:
        assert day_only.shape == (96, 37)


def test_real_rf_training_saves_reloadable_day_only_candidate(tmp_path, monkeypatch):
    import joblib
    import json
    import yaml
    from src.models import model_registry, train_tabular

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "stations.yaml").write_text(yaml.safe_dump({"stations": {"demo": {
        "training_file": "source.csv", "interval_minutes": 15,
        "replay_start_date": "2025-02-01T00:00:00Z",
    }}}))
    (config_dir / "model.yaml").write_text("history_window_hours: 168\nforecast_horizon_hours: 24\n")
    pd.DataFrame({
        "utc_timestamp": pd.date_range("2025-01-01T00:00:00Z", periods=2000, freq="15min"),
        "demand": 1000 + np.arange(2000), "temperature": 10.0,
    }).to_csv(tmp_path / "source.csv", index=False)
    monkeypatch.setattr(train_tabular, "ROOT", tmp_path)
    monkeypatch.setattr(model_registry, "REGISTRY_PATH", tmp_path / "registry.json")
    metadata = train_tabular.run_training({
        "station": "demo", "algorithm": "random_forest", "experiment_id": "rf-smoke",
        "parameters": {"n_estimators": 2, "max_depth": 2, "random_state": 42,
                       "max_train_origins": 3, "max_validation_origins": 2},
    })
    saved = json.loads((tmp_path / metadata["metadata_path"]).read_text())
    model = joblib.load(tmp_path / metadata["artifact_path"])
    assert saved["feature_schema"] == DAY_ONLY_FEATURE_SCHEMA
    assert len(saved["feature_names"]) == model.n_features_in_ == 37
    assert TARGET_DAY_FEATURE in saved["feature_names"]
    assert TARGET_WEEK_FEATURE not in saved["feature_names"]
    assert "demand_lag_672" in saved["feature_names"]
    assert saved["validation_samples"] == saved["test_metrics"]["matched_observations"] == 192
    assert model_registry.get_experiment("rf-smoke")["status"] == "candidate"
    history, targets = history_and_targets()
    features = build_origin_features(history, targets, feature_schema=saved["feature_schema"])
    assert np.isfinite(model.predict(features[saved["feature_names"]].to_numpy(dtype=np.float32))).all()
