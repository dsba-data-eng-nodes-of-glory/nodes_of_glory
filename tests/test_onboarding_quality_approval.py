"""Quality approval gates registration, weather, Kafka topics and deployment."""
import asyncio
import io
import time

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile

from backend import onboarding_jobs as jobs, onboarding_quality, station_api


def until(predicate, seconds=4):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(.03)
    return False


def begin(monkeypatch, tmp_path, station, callback):
    monkeypatch.setattr(jobs, "ROOT", tmp_path)
    registry = {"stations": {}}
    monkeypatch.setattr(jobs, "read_station_config", lambda: registry)
    monkeypatch.setattr(station_api, "onboard_station", callback)
    result = asyncio.run(jobs.enqueue_job(
        station=station,
        upload=UploadFile(io.BytesIO(b"timestamp,demand\n2026-09-30,1\n"), filename="raw.csv"),
        values={"selected_models": "demand_forecast"},
    ))
    return result["job_id"], registry


def test_quality_summary_includes_training_compromise():
    report = {
        "data_accepted": True, "training_ready": False, "rows": 1000,
        "missing_timestamp_slots": 3, "missing_timestamp_pct": .3,
        "target_missing_pct_before_fill": {"demand": 1.2},
        "source_malformed_cells": 4,
        "wind_generation_exceeds_reported_capacity_intervals": 22,
        "imputed_cell_counts": {"demand": 8},
        "warnings": ["Reviewed generation exceeds installed capacity"],
    }
    review = onboarding_quality.build_quality_review(report, "station_a")
    assert review["training_risk"] and not review["training_ready"]
    assert review["warning_count"] >= 4
    assert review["rows"] == 1000
    assert "authorize registration" in review["approval_statement"].lower()


def test_clean_data_still_requires_confirmation():
    report = {"data_accepted": True, "training_ready": True, "rows": 200}
    review = onboarding_quality.build_quality_review(report, "station_a")
    assert review["training_risk"] is False
    with pytest.raises(ValueError):
        onboarding_quality.build_quality_review({"data_accepted": False}, "station_b")


def test_approve_unblocks_and_activates_registration_without_page(monkeypatch, tmp_path):
    side_effects = []
    async def onboard(file, **kwargs):
        jobs.progress("spark", "Spark cleaning")
        assert await file.read()
        quality = onboarding_quality.build_quality_review({
            "data_accepted": True, "training_ready": False,
            "rows": 200, "source_malformed_cells": 2,
        }, "test_station")
        await jobs.request_quality_confirmation(quality)
        side_effects.append("weather")
        jobs.progress("weather", "Fetching weather")
        side_effects.append("registry")
        registry["stations"]["test_station"] = {"initial_training": {"demand_forecast": {
            "status": "queued", "algorithm": "dense_nn"}}}
        side_effects.append("training")
        return {"station": "test_station", "registered": True, "topics_ready": True,
                "enabled_models": ["demand_forecast"]}

    jid, registry = begin(monkeypatch, tmp_path, "test_station", onboard)
    assert until(lambda: jobs.get_job(jid)["status"] == "awaiting_confirmation")
    assert side_effects == []
    assert not registry["stations"]
    job = jobs.get_job(jid)
    assert job["quality_review"]["issues"][0]["severity"] == "warning"
    assert job["stage"] == "quality_review"
    assert jid in {j["id"] for j in jobs.recent_jobs()}
    assert jobs.decide_quality_confirmation(jid, approved=True)["decision"] == "approved"
    with pytest.raises(HTTPException) as error:
        jobs.decide_quality_confirmation(jid, approved=True)
    assert error.value.status_code == 409
    assert until(lambda: jobs.get_job(jid)["status"] == "training")
    assert side_effects == ["weather", "registry", "training"]
    registry["stations"]["test_station"]["initial_training"]["demand_forecast"]["status"] = "ready"
    assert jobs.get_job(jid)["status"] == "completed"


def test_decline_cancels_without_registration(monkeypatch, tmp_path):
    touched = []
    async def onboard(file, **kwargs):
        await jobs.request_quality_confirmation(onboarding_quality.build_quality_review({
            "data_accepted": True, "training_ready": False,
            "rows": 500, "wind_generation_exceeds_reported_capacity_intervals": 10,
        }, "reject_station"))
        touched.append("registered")
        return {"station": "reject_station", "registered": True}

    jid, registry = begin(monkeypatch, tmp_path, "reject_station", onboard)
    assert until(lambda: jobs.get_job(jid)["status"] == "awaiting_confirmation")
    jobs.decide_quality_confirmation(jid, approved=False)
    assert until(lambda: jobs.get_job(jid)["status"] == "cancelled")
    assert touched == []
    assert registry["stations"] == {}
    assert not (tmp_path / 'data' / 'raw' / 'stations' / 'reject_station').exists()
    assert not (tmp_path / 'data' / 'state' / 'onboarding_jobs' / jid / 'customer_source.csv').exists()


def test_worker_releases_semaphore_while_approval_pending(monkeypatch, tmp_path):
    entered = []
    async def onboard(file, **kwargs):
        station = kwargs["station_id"]
        entered.append(station)
        await jobs.request_quality_confirmation(onboarding_quality.build_quality_review({
            "data_accepted": True, "training_ready": True, "rows": 700,
        }, station))
        return {"station": station, "registered": True, "enabled_models": []}

    j1, reg = begin(monkeypatch, tmp_path, "a_station", onboard)
    assert until(lambda: jobs.get_job(j1)["status"] == "awaiting_confirmation")
    j2, _ = begin(monkeypatch, tmp_path, "b_station", onboard)
    assert until(lambda: jobs.get_job(j2)["status"] == "awaiting_confirmation")
    assert set(entered) == {"a_station", "b_station"}
    jobs.decide_quality_confirmation(j1, approved=False)
    jobs.decide_quality_confirmation(j2, approved=False)
    assert until(lambda: jobs.get_job(j1)["status"] == "cancelled")
    assert until(lambda: jobs.get_job(j2)["status"] == "cancelled")


def test_direct_raw_registration_cannot_bypass_gate():
    with pytest.raises(HTTPException) as error:
        asyncio.run(jobs.request_quality_confirmation({"data_accepted": True}))
    assert error.value.status_code == 409
