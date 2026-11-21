"""Durable, page-independent station onboarding jobs.

SQLite here holds *job status*, not forecast storage.  The actual validation,
Spark, weather, station registration and initial-training code is unchanged.

Jobs survive Streamlit page navigation and browser disconnects. A FastAPI process
restart marks in-flight registration jobs INTERRUPTED (not falsely successful).
Previously registered stations can continue training through the normal manager.
Run a single Uvicorn worker for this local implementation.
"""
from __future__ import annotations

import asyncio
import contextvars
import json
import os
import sqlite3
import threading
import time
import traceback
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

from fastapi import HTTPException
from starlette.datastructures import UploadFile

from backend.pipeline_manager import ROOT, read_station_config

_RUN_ID = str(uuid.uuid4())
_LOCK = threading.RLock()
# Avoid overlapping standalone Spark cleans / weather cache writers on 8 GB laptops.
_SEMAPHORE = threading.Semaphore(1)
_PROGRESS = contextvars.ContextVar("onboarding_progress_callback", default=None)
_JOB_CONTEXT = contextvars.ContextVar("onboarding_quality_job", default=None)
_QUALITY_TIMEOUT_SECONDS = 3600  # Prevent an unattended review from holding raw data in RAM indefinitely.


class RegistrationDeclined(Exception):
    """Customer declined the quality warning or the approval window expired."""

ACTIVE = {"queued", "processing", "awaiting_confirmation", "cancelling", "training"}
STAGES = {
    "accepted": 1, "waiting": 2, "validation": 3, "spark": 4,
    "quality": 5, "quality_review": 5, "weather": 6, "preparation": 7, "registry": 8,
    "topics": 9, "training": 10, "completed": 11,
}


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _dir() -> Path:
    root = ROOT / "data" / "state" / "onboarding_jobs"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _connect():
    db = sqlite3.connect(str(_dir() / "jobs.sqlite3"), timeout=25)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout = 25000")
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("""CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY, station TEXT NOT NULL, status TEXT NOT NULL,
        stage TEXT NOT NULL, message TEXT NOT NULL,
        created TEXT NOT NULL, updated TEXT NOT NULL, run_id TEXT NOT NULL,
        upload_path TEXT NOT NULL, original_name TEXT NOT NULL,
        parameters TEXT NOT NULL, result TEXT, error TEXT
    )""")
    # Separate table retains the original jobs schema and old INSERT clients.
    db.execute("""CREATE TABLE IF NOT EXISTS quality_reviews (
        job_id TEXT PRIMARY KEY, report TEXT NOT NULL,
        decision TEXT, updated TEXT NOT NULL
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
        created TEXT NOT NULL, stage TEXT NOT NULL, message TEXT NOT NULL
    )""")
    db.commit()
    return db


def _change(job_id: str, status: str, stage: str, message: str, result=None, error=None):
    with _LOCK, _connect() as db:
        now = _utc()
        db.execute("""UPDATE jobs SET status=?,stage=?,message=?,updated=?,
                   result=COALESCE(?,result),error=COALESCE(?,error) WHERE id=?""",
                   (status, stage, message, now,
                    json.dumps(result) if result is not None else None,
                    str(error) if error is not None else None, job_id))
        db.execute("INSERT INTO events(job_id,created,stage,message) VALUES(?,?,?,?)",
                   (job_id, now, stage, message))
        db.commit()


def progress(stage: str, message: str) -> None:
    callback = _PROGRESS.get()
    if callback is not None:
        callback(stage, message)


def in_onboarding_job() -> bool:
    """True only for a backend-owned registration job requiring quality approval."""
    return _JOB_CONTEXT.get() is not None


def _record(row: sqlite3.Row) -> dict:
    result = dict(row)
    result["result"] = json.loads(result.pop("result") or "{}")
    result.pop("parameters", None)
    result.pop("upload_path", None)
    result.pop("original_name", None)
    result.pop("run_id", None)
    return result


def _event_once(job_id: str, stage: str, message: str) -> None:
    with _LOCK, _connect() as db:
        exists = db.execute(
            "SELECT 1 FROM events WHERE job_id=? AND stage=? LIMIT 1", (job_id, stage)
        ).fetchone()
        if not exists:
            db.execute("INSERT INTO events(job_id,created,stage,message) VALUES(?,?,?,?)",
                       (job_id, _utc(), stage, message))
            db.commit()


def _training_update(job: dict) -> dict:
    # Continue to expose per-model details after completion/failure as well.
    if job["status"] in {"queued", "processing", "awaiting_confirmation", "cancelling", "interrupted", "cancelled"}:
        return job
    config = read_station_config()
    entry = (config.get("stations") or {}).get(job["station"])
    if not entry:
        return job
    tasks = entry.get("initial_training") or {}
    if not tasks:
        return job
    total = len(tasks)
    done = sum(t.get("status") == "ready" for t in tasks.values())
    failed = [(k, t.get("error") or "Training failed") for k, t in tasks.items()
              if t.get("status") == "failed"]
    running = [k for k, t in tasks.items() if t.get("status") == "running"]
    job["models"] = tasks
    job["models_ready"] = done
    job["models_total"] = total
    for component, task in tasks.items():
        status = task.get("status", "unknown")
        if status in {"running", "ready", "failed"}:
            _event_once(job["id"], f"model:{component}:{status}",
                        f"{component.replace('_',' ').title()}: {status.replace('_',' ').title()}")
    if job["status"] in {"completed", "failed"}:
        return job
    if failed:
        msg = f"{done}/{total} deployed; {failed[0][0]} failed: {failed[0][1]}"
        if job["stage"] != "training_failed" or job.get("message") != msg:
            _change(job["id"], "failed", "training_failed", msg, error=msg)
        job.update(status="failed", stage="training_failed", message=msg, error=msg)
    elif done == total:
        msg = f"All selected models deployed ({done}/{total}). Station is ready to activate."
        _change(job["id"], "completed", "completed", msg)
        job.update(status="completed", stage="completed", message=msg)
    else:
        stage_info = f"Training and deployment: {done}/{total} ready"
        if running:
            stage_info += f" · running {running[0]}"
        job["message"] = stage_info
    return job


def _mark_interrupted(job: dict) -> dict:
    with _LOCK, _connect() as db:
        row = db.execute("SELECT run_id FROM jobs WHERE id=?", (job["id"],)).fetchone()
    if row and row["run_id"] != _RUN_ID and job["status"] in {"queued", "processing", "awaiting_confirmation", "cancelling"}:
        msg = "FastAPI restarted during registration. Check station registry before resubmitting."
        _change(job["id"], "interrupted", "interrupted", msg, error=msg)
        job.update(status="interrupted", stage="interrupted", message=msg, error=msg)
    return job


def get_job(job_id: str) -> dict:
    with _LOCK, _connect() as db:
        row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "Onboarding job not found")
    job = _training_update(_mark_interrupted(_record(row)))
    # Read events AFTER training transitions have been recorded.
    with _LOCK, _connect() as db:
        events = [dict(r) for r in db.execute(
            "SELECT created,stage,message FROM events WHERE job_id=? ORDER BY id DESC LIMIT 40", (job_id,))]
        review_row = db.execute("SELECT report,decision FROM quality_reviews WHERE job_id=?", (job_id,)).fetchone()
    job["quality_review"] = json.loads(review_row["report"]) if review_row else {}
    job["quality_decision"] = review_row["decision"] if review_row else None
    job["events"] = list(reversed(events))
    return job


def recent_jobs(limit: int = 10) -> list[dict]:
    with _LOCK, _connect() as db:
        rows = db.execute("SELECT * FROM jobs ORDER BY created DESC, rowid DESC LIMIT ?", (min(max(limit, 1), 50),)).fetchall()
    result = []
    for r in rows:
        job = _training_update(_mark_interrupted(_record(r)))
        result.append(job)
    return result


def decide_quality_confirmation(job_id: str, *, approved: bool) -> dict:
    """Atomic, single-use approval endpoint. A decision is not a UI-only flag."""
    with _LOCK, _connect() as db:
        row = db.execute("SELECT station,status,run_id FROM jobs WHERE id=?", (job_id,)).fetchone()
        review = db.execute("SELECT report,decision FROM quality_reviews WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "Onboarding job not found")
        if row["status"] != "awaiting_confirmation" or (review and review["decision"] is not None):
            raise HTTPException(409, "This job is no longer awaiting quality approval")
        if row["run_id"] != _RUN_ID:
            raise HTTPException(409, "Backend restarted; the quality review must be rerun")
        if not review or not review["report"]:
            raise HTTPException(409, "Quality review is not ready")
        action = "approved" if approved else "declined"
        now = _utc()
        db.execute("UPDATE quality_reviews SET decision=?,updated=? WHERE job_id=?", (action, now, job_id))
        db.execute("UPDATE jobs SET updated=?,message=? WHERE id=?",
                   (now, "Quality review accepted; resuming registration" if approved
                    else "Customer declined registration; cleaning up unregistered files", job_id))
        db.execute("INSERT INTO events(job_id,created,stage,message) VALUES(?,?,?,?)",
                   (job_id, now, "quality_review", "Customer " + action + " the data-quality review"))
        db.commit()
    return {"job_id": job_id, "station": row["station"], "decision": action}


def _wait_for_quality_decision(job_id: str, quality_review: dict) -> None:
    """Pause before any weather, registry or model side effects.

    Release the *single-station Spark* semaphore while the customer decides;
    other uploads are not blocked by an unattended confirmation dialog.
    """
    with _LOCK, _connect() as db:
        now = _utc()
        db.execute("INSERT INTO quality_reviews(job_id,report,decision,updated) VALUES(?,?,NULL,?) "
                   "ON CONFLICT(job_id) DO UPDATE SET report=excluded.report,decision=NULL,updated=excluded.updated",
                   (job_id, json.dumps(quality_review), now))
        db.execute("UPDATE jobs SET status='awaiting_confirmation',stage='quality_review',"
                   "message=?,updated=? WHERE id=?",
                   ("Waiting for customer review of Spark quality findings", now, job_id))
        db.execute("INSERT INTO events(job_id,created,stage,message) VALUES(?,?,?,?)",
                   (job_id, now, "quality_review", "Spark audit ready; awaiting explicit customer confirmation"))
        db.commit()
    _SEMAPHORE.release()
    try:
        deadline = time.monotonic() + _QUALITY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            with _LOCK, _connect() as db:
                row = db.execute("SELECT decision FROM quality_reviews WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise RegistrationDeclined("Registration job no longer exists")
            if row["decision"] == "approved":
                return
            if row["decision"] == "declined":
                raise RegistrationDeclined("Customer declined the data-quality review; station not registered")
            time.sleep(0.25)
        raise RegistrationDeclined("Quality confirmation expired after one hour; station not registered")
    finally:
        _SEMAPHORE.acquire()


async def request_quality_confirmation(quality_review: dict) -> None:
    """Only background-job submissions can continue after explicit customer review.

    Historical direct-onboard clients are not implicitly approved by this function;
    the customer portal always uses the background-job endpoint.
    """
    job_id = _JOB_CONTEXT.get()
    if job_id is None:
        # Disallow bypass of the quality review through the legacy raw endpoint.
        raise HTTPException(409, "Raw registration requires /stations/onboard/jobs and quality approval")
    await asyncio.to_thread(_wait_for_quality_decision, job_id, quality_review)
    _change(job_id, "processing", "quality", "Quality approved; proceeding to weather and data preparation")


async def enqueue_job(*, station: str, upload: UploadFile, values: dict[str, str]) -> dict:
    from backend.station_api import _station_key
    station = _station_key(station)
    if station in (read_station_config().get("stations") or {}):
        raise HTTPException(409, f"Station '{station}' is already registered")
    ext = Path(upload.filename or "").suffix.lower()
    if ext not in {".csv", ".xlsx", ".xlsm"}:
        raise HTTPException(422, "Upload one CSV/XLSX/XLSM file")
    with _LOCK:
        if any(j["station"] == station and j["status"] in ACTIVE for j in recent_jobs(50)):
            raise HTTPException(409, "Registration is already in progress for this station")
        job_id = uuid.uuid4().hex
        folder = _dir() / job_id
        folder.mkdir(parents=True, exist_ok=False)
        source = folder / ("customer_source" + ext)
        size = 0
        try:
            with source.open("wb") as output:
                while True:
                    chunk = await upload.read(1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > 200 * 1024 * 1024:
                        raise HTTPException(413, "Upload exceeds the 200 MB registration limit")
                    output.write(chunk)
            if size == 0:
                raise HTTPException(422, "Uploaded file is empty")
            now = _utc()
            with _connect() as db:
                db.execute("""INSERT INTO jobs(id,station,status,stage,message,created,updated,run_id,
                             upload_path,original_name,parameters) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                           (job_id, station, "queued", "accepted", "Raw file received by backend", now, now,
                            _RUN_ID, str(source), upload.filename, json.dumps(values)))
                db.execute("INSERT INTO events(job_id,created,stage,message) VALUES(?,?,?,?)",
                           (job_id, now, "accepted", f"Uploaded {size:,} bytes; accepted into backend queue"))
                db.commit()
        except BaseException:
            source.unlink(missing_ok=True)
            folder.rmdir()
            raise
    threading.Thread(target=_run_job, args=(job_id,), name=f"onboard-{station}", daemon=True).start()
    return {"job_id": job_id, "station": station, "status": "queued", "message": "Upload accepted; backend owns registration"}


def _run_job(job_id: str) -> None:
    with _SEMAPHORE:
        with _LOCK, _connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            return
        station = row["station"]
        _change(job_id, "processing", "validation", "Validating registration settings")
        token = _PROGRESS.set(lambda stage, text: _change(job_id, "processing", stage, text))
        quality_token = _JOB_CONTEXT.set(job_id)
        try:
            from backend.station_api import onboard_station
            parameters = json.loads(row["parameters"])
            parameters["station_id"] = station
            if parameters.get("replay_start_date"):
                parameters["replay_start_date"] = date.fromisoformat(parameters["replay_start_date"])
            else:
                parameters["replay_start_date"] = None
            parameters["stream_fraction"] = float(parameters.get("stream_fraction") or 0.2)
            parameters["interval_minutes"] = int(parameters.get("interval_minutes") or 15)
            # Fill all FastAPI-form defaults with plain values (no Form instances).
            defaults = {
                "display_name": None, "timestamp_format": "auto", "source_timezone": "UTC",
                "selected_models": "", "stream_input": "csv_replay", "residual_demand_unit": "mwh_per_interval",
                "raw_topic": None, "featured_topic": None, "hierarchy_path": "",
                "weather_locations": "", "numeric_locale": "de", "raw_sheet_name": "",
                "raw_column_overrides": "", "upload_mode": "raw",
            }
            defaults.update(parameters)
            with open(row["upload_path"], "rb") as saved:
                upload = UploadFile(file=saved, filename=row["original_name"])
                result = asyncio.run(onboard_station(file=upload, **defaults))
            slim = {k: result.get(k) for k in ("station", "registered", "topics_ready", "topic_warning",
                    "raw_spark_quality", "replay_start_date", "enabled_models")}
            _change(job_id, "training", "training", "Station registered; models queued for training and deployment", result=slim)
            # The original source is now archived in the registered station folder;
            # do not retain a second 200 MB copy for every successful upload.
            Path(row["upload_path"]).unlink(missing_ok=True)
        except RegistrationDeclined as exc:
            # Customer cancellation must not leave the uploaded raw source behind.
            Path(row["upload_path"]).unlink(missing_ok=True)
            _change(job_id, "cancelled", "cancelled", str(exc))
        except Exception as exc:
            text = str(exc.detail) if isinstance(exc, HTTPException) else str(exc)
            _change(job_id, "failed", "failed", text, error=text)
            # The server log contains the traceback; the dashboard displays the reason only.
            import logging
            logging.getLogger(__name__).exception("Onboarding failed for %s", station)
        finally:
            _PROGRESS.reset(token)
            _JOB_CONTEXT.reset(quality_token)
