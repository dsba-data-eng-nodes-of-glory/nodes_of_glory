"""Mocked endpoint path: raw CSV -> Spark -> station registry -> replay files."""
from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from io import BytesIO

import pandas as pd
from starlette.datastructures import UploadFile
from backend import station_api


def test_raw_station_creates_training_and_replay(monkeypatch, tmp_path):
    state = {"active_station": None, "stations": {}}
    monkeypatch.setattr(station_api, "ROOT", tmp_path)
    monkeypatch.setattr(station_api, "_read_model_settings", lambda: {
        "interval_minutes": 15, "history_window_hours": 168, "forecast_horizon_hours": 24,
    })
    monkeypatch.setattr(station_api, "read_station_config", lambda: deepcopy(state))
    def save(data):
        state.clear()
        state.update(deepcopy(data))
    monkeypatch.setattr(station_api, "write_station_config", save)
    monkeypatch.setattr(station_api, "_validate_model_row_counts", lambda *args: {"mocked": "validated"})
    class Topics:
        def ensure_station_topics(self, name):
            return {"station": name, "created": True}
    monkeypatch.setattr(station_api, "manager", Topics())

    def fake_clean(source, staging, project_root, station_id, **kwargs):
        n = 2400
        ts = pd.date_range("2026-09-15T00:00:00Z", periods=n, freq="15min")
        cleaned = source.with_name(source.stem + "_cleaned.csv")
        pd.DataFrame({
            "timestamp": ts.strftime("%d-%m-%Y %H:%M"),
            "demand": 1000 + pd.Series(range(n)) * .02,
            "Photovoltaics [MWh]": 20.0,
            "Photovoltaics_installed_capacity [MW]": 200.0,
            "Wind onshore [MWh]": 10.0,
            "Wind_onshore_installed_capacity [MW]": 200.0,
        }).to_csv(cleaned,index=False)
        report = {"data_accepted":True,"rows":n}
        source.with_name(source.stem + "_quality.json").write_text(json.dumps(report))
        return cleaned,report,tmp_path/"unused.log"
    monkeypatch.setattr(station_api,"spark_clean_raw_source",fake_clean)

    class Weather:
        def enrich(self,datasets):
            datasets["demand"]["temperature"]=10.0
            datasets["demand"]["forecast_temperature"]=11.0
    monkeypatch.setattr(station_api,"station_weather",lambda *args: Weather())
    upload = UploadFile(filename="customer_data.csv", file=BytesIO(b"source,extra\n1,abc\n"))
    from datetime import date
    result = asyncio.run(station_api.onboard_station(
        station_id="demo-raw",
        file=upload,
        display_name="Demo Raw",
        stream_input="csv_replay",
        replay_start_date=date(2026,10,1),
        selected_models="demand_forecast",
        source_timezone="auto",
        weather_locations="Dortmund, 51.5136, 7.4653",
        numeric_locale="de", upload_mode="raw",
        interval_minutes=15, raw_topic=None, featured_topic=None,
    ))
    entry=state["stations"]["demo-raw"]
    assert result["registered"]
    assert result["topics_ready"]
    assert entry["source_format"]=="raw"
    assert entry["weather"]["sites"][0]["location"]=="Dortmund"
    assert entry["stream_start_utc"] == "2026-10-01T00:00:00+00:00"
    assert (tmp_path/entry["training_file"]).is_file()
    assert (tmp_path/entry["input_file"]).is_file()
    assert (tmp_path/entry["archived_source_file"]).is_file()
    train=pd.read_csv(tmp_path/entry["training_file"])
    replay=pd.read_csv(tmp_path/entry["input_file"])
    assert train["utc_timestamp"].max() < "2026-10-01T00:00:00Z"
    assert replay["utc_timestamp"].min() == "2026-10-01T00:00:00Z"
