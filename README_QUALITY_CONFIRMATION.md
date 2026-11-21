# Nodes of Glory — Spark Data Quality Confirmation Gate

**Patch applies after:** Raw One-File Spark Patch V2 + Station Location Acceptor + Background Registration Progress + Import-Path Hotfix.

## What changes

1. The customer uploads one raw CSV/XLSX file via Station Management.
2. FastAPI owns a durable background job; Spark validates, cleans and produces its JSON quality audit.
3. If Spark **hard-rejects** the file (for example, 10%+ required-target coverage failure), onboarding fails. This is never overrideable.
4. If Spark accepts it, **the entire registration job pauses** at `awaiting_confirmation` and saves its quality report in `data/state/onboarding_jobs/jobs.sqlite3`, separate from existing job tables.
5. The Station Management page pops up **Data quality review — approve before model training** and shows issues (severity, nature, potential effect). The customer must check an acknowledgement box to enable **Confirm and continue**, or choose **Cancel registration**. Even an accepted file with no issues still requires acknowledgement.
6. On confirmation only, FastAPI resumes its weather calls, model-data preparation, YAML registration, Kafka topic creation and automatic model training and deployment.
7. On cancellation, FastAPI cleans the temporary registration work and marks the job cancelled. No station or model deployment is written.
8. The reminder is visible on other pages; you can return to Station Management, reopen the review, and make a decision. An unconfirmed review expires after one hour to free memory. If FastAPI restarts while waiting, the job reports **Interrupted**; it is never silently approved.

### Training compromise warning

The review explicitly distinguishes `Spark accepted` from `training_ready`. Examples: repaired timestamps, imputed Demand/Solar/Wind Energy readings, duplicate rows, malformed values, demand flatlines, generation > installed capacity, remaining unfilled values, unverified units and original Spark warnings. These may compromise model quality even if Spark passes the source acceptance limit. Confirmation does **not** mean that a model is reliable, and later weather/model validation can still fail.

## Installation (Windows)

1. Stop FastAPI and Streamlit; preserve your `config`, `data`, `models` and checkpoints folders.
2. Extract the ZIP into a temporary directory. Copy the **contents of `overlay/`** to your project root, overwriting the five matching code files only:
   - `backend/onboarding_jobs.py`
   - `backend/onboarding_quality.py` (new)
   - `backend/station_api.py`
   - `dashboard/pages/station_management.py`
   - `dashboard/ui.py`
3. Start FastAPI with one Uvicorn worker, then restart Streamlit.
4. Open **Station Management → Register a station** and submit a sample raw file. The progress list should reach **Customer quality approval**, showing **Awaiting customer decision**. Registration will not advance until you confirm.
5. Check both branches: **Cancel** (no new station) and then retry with same ID and **Confirm** (weather → registry → Kafka → training). While pending, use another dashboard page; the approval reminder should remain visible.

### API contract

- `GET /stations/onboard/jobs/{job_id}` returns `status: awaiting_confirmation`, `quality_review` containing `issues`, `training_risk`, `training_ready` and summary.
- `POST /stations/onboard/jobs/{job_id}/quality-decision` JSON body `{"approved": true}` or `{"approved": false}`. Single-use decision; second attempt returns HTTP 409.
- **Direct raw uploads via `POST /stations/onboard` are intentionally disallowed**, because they bypass the customer review. All new raw registrations must use the background job endpoint. Older tests or integrations calling the direct raw endpoint should be updated to submit a job and explicitly confirm it. Legacy-template handling outside the raw path is unchanged.

### Test evidence

- 10 focused tests pass, including real station registration with mocked Spark and weather I/O, plus background job review/approval/cancellation, no early side effects and sequential Spark work across multiple registrations.
- Real Spark executor, Open-Meteo, Windows browser pop-up and full Kafka pipeline have **not** been verified in this environment. Test on the destination laptop before customer demonstration.

## Rollback

Restore these five source files from your backup. The new `quality_reviews` table in the existing SQLite job database is harmless and can be left in place; older job rows are preserved.
