"""Integration-level flow: source file -> cleaned datasets -> approval -> registration.
Mocks Spark and weather network only; uses the real station registration function.
"""
import asyncio
import io
import json
import time
from copy import deepcopy
from datetime import date

import pandas as pd
from starlette.datastructures import UploadFile
from backend import station_api, onboarding_jobs as jobs


def until(fn, timeout=7):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if fn():
            return True
        time.sleep(.035)
    return False


def test_real_onboard_pauses_before_weather_and_registry(monkeypatch, tmp_path):
    state = {"active_station": None, "stations": {}}
    monkeypatch.setattr(station_api, "ROOT", tmp_path)
    monkeypatch.setattr(jobs, "ROOT", tmp_path)
    monkeypatch.setattr(jobs, "read_station_config", lambda: deepcopy(state))
    monkeypatch.setattr(station_api, "read_station_config", lambda: deepcopy(state))
    monkeypatch.setattr(station_api, "write_station_config", lambda new: (state.clear(), state.update(deepcopy(new))))
    monkeypatch.setattr(station_api, "_read_model_settings", lambda: {
        "interval_minutes": 15, "history_window_hours": 168, "forecast_horizon_hours": 24,
    })
    monkeypatch.setattr(station_api, "_validate_model_row_counts", lambda *args: {"mocked": "validated"})
    calls = []
    class Topics:
        def ensure_station_topics(self, station):
            calls.append("topics")
            return {"station": station, "created": True}
    monkeypatch.setattr(station_api, "manager", Topics())

    def clean(source, staging, root, station, **kwargs):
        calls.append("spark")
        n = 2400
        ts = pd.date_range("2026-09-15T00:00:00Z", periods=n, freq="15min")
        clean_path = source.with_name(source.stem + "_cleaned.csv")
        pd.DataFrame({
            "timestamp": ts.strftime("%d-%m-%Y %H:%M"),
            "demand": 500 + pd.Series(range(n)) * .05,
            "Photovoltaics [MWh]": 20.0,
            "Photovoltaics_installed_capacity [MW]": 1000,
            "Wind onshore [MWh]": 12.0,
            "Wind_onshore_installed_capacity [MW]": 1000,
        }).to_csv(clean_path, index=False)
        report = {"data_accepted": True, "training_ready": False, "rows": n,
                  "wind_generation_exceeds_reported_capacity_intervals": 5,
                  "warnings": ["Verify wind capacity"]}
        clean_path.with_name(source.stem + "_quality.json").write_text(json.dumps(report))
        return clean_path, report, tmp_path / "spark.log"
    monkeypatch.setattr(station_api, "spark_clean_raw_source", clean)
    class Weather:
        def enrich(self, datasets):
            calls.append("weather")
            datasets["demand"]["temperature"] = 10.0
            datasets["demand"]["forecast_temperature"] = 11.0
    monkeypatch.setattr(station_api, "station_weather", lambda *args: Weather())

    upload = UploadFile(filename="client.csv", file=io.BytesIO(b"original,extra\nA,B\n"))
    jid = asyncio.run(jobs.enqueue_job(station="sub01", upload=upload, values={
        "station_id": "sub01", "display_name": "Substation 01",
        "stream_input": "csv_replay", "replay_start_date": date(2026, 10, 1).isoformat(),
        "selected_models": "demand_forecast", "source_timezone": "auto", "upload_mode": "raw",
        "weather_locations": "Dortmund, 51.5136, 7.4653", "interval_minutes": "15",
        "numeric_locale": "de",
    }))["job_id"]
    assert until(lambda: jobs.get_job(jid)["status"] == "awaiting_confirmation")
    assert calls == ["spark"]  # no weather, registry, topics or models before approval
    assert not state["stations"]
    assert jobs.get_job(jid)["quality_review"]["warning_count"] > 0

    jobs.decide_quality_confirmation(jid, approved=True)
    assert until(lambda: jobs.get_job(jid)["status"] in {"training", "failed"})
    job = jobs.get_job(jid)
    assert job["status"] == "training", job
    assert calls == ["spark", "weather", "topics"]
    assert state["stations"]["sub01"]["source_format"] == "raw"
    assert (tmp_path / state["stations"]["sub01"]["training_file"]).is_file()
    assert (tmp_path / state["stations"]["sub01"]["input_file"]).is_file()
