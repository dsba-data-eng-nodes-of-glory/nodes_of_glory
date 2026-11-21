"""Simulated backend job tests: page-independent progress, success/failure and restart."""
import asyncio
import io
import time

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile

from backend import onboarding_jobs as jobs, station_api


def wait_until(predicate, seconds=3):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_upload_returns_immediately_and_tracks_training(monkeypatch, tmp_path):
    state = {"stations": {}}
    monkeypatch.setattr(jobs, "ROOT", tmp_path)
    monkeypatch.setattr(jobs, "read_station_config", lambda: state)
    async def slow_onboard(file, **kwargs):
        assert await file.read() == b"time,demand\n1,42\n"
        jobs.progress("spark", "Spark preprocessing")
        await asyncio.sleep(.18)
        jobs.progress("weather", "Historical weather")
        await asyncio.sleep(.12)
        state["stations"]["station01"] = {"initial_training": {
            "demand_forecast": {"status": "queued", "algorithm": "dense_nn"},
            "solar_forecast": {"status": "queued", "algorithm": "lstm"},
        }}
        return {"station": "station01", "registered": True, "topics_ready": True,
                "enabled_models": ["demand_forecast", "solar_forecast"]}
    monkeypatch.setattr(station_api, "onboard_station", slow_onboard)
    start = time.monotonic()
    result = asyncio.run(jobs.enqueue_job(
        station="station01", upload=UploadFile(io.BytesIO(b"time,demand\n1,42\n"), filename="raw.csv"),
        values={"selected_models": "demand_forecast,solar_forecast"},
    ))
    assert time.monotonic() - start < .15
    jid = result["job_id"]
    assert wait_until(lambda: jobs.get_job(jid)["status"] == "training")
    assert "spark" in {e["stage"] for e in jobs.get_job(jid)["events"]}
    state["stations"]["station01"]["initial_training"]["demand_forecast"]["status"] = "ready"
    halfway = jobs.get_job(jid)
    assert halfway["models_ready"] == 1
    assert halfway["models_total"] == 2
    state["stations"]["station01"]["initial_training"]["solar_forecast"]["status"] = "ready"
    finished = jobs.get_job(jid)
    assert finished["status"] == "completed"
    assert len([e for e in finished["events"] if e["stage"].startswith("model:")]) >= 2
    assert len(jobs.recent_jobs()) == 1


def test_background_failure_shows_error_and_no_false_completion(monkeypatch, tmp_path):
    monkeypatch.setattr(jobs, "ROOT", tmp_path)
    monkeypatch.setattr(jobs, "read_station_config", lambda: {"stations": {}})
    async def failed_onboard(file, **kwargs):
        jobs.progress("weather", "Fetching forecast weather")
        raise HTTPException(status_code=503, detail="Weather HTTP 429; retry later")
    monkeypatch.setattr(station_api, "onboard_station", failed_onboard)
    jid = asyncio.run(jobs.enqueue_job(
        station="station02", upload=UploadFile(io.BytesIO(b"a,b\n1,2\n"), filename="raw.csv"),
        values={"selected_models": "demand_forecast"},
    ))["job_id"]
    assert wait_until(lambda: jobs.get_job(jid)["status"] == "failed")
    assert "429" in jobs.get_job(jid)["message"]
    assert jobs.get_job(jid)["status"] != "completed"


def test_restart_marks_unfinished_processing_interrupted(monkeypatch, tmp_path):
    monkeypatch.setattr(jobs, "ROOT", tmp_path)
    monkeypatch.setattr(jobs, "read_station_config", lambda: {"stations": {}})
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    with jobs._connect() as db:
        db.execute("INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,NULL,NULL)",
            ("older", "substation01", "processing", "weather", "Fetching weather", now, now,
             "previous-run-id", "raw.csv", "raw.csv", "{}"))
    j = jobs.get_job("older")
    assert j["status"] == "interrupted"
    assert "restarted" in j["message"].lower()
