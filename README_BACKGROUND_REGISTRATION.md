# Nodes of Glory — background registration and transparent progress

**Baseline:** `Performance upgrade(2).zip` **plus**:
1. `Nodes_of_Glory_Raw_One_File_Spark_Patch_V2.zip`
2. `Nodes_of_Glory_Station_Location_Acceptor_Patch.zip`

This is an **incremental patch**. No model, weather, Spark algorithm, station YAML, topic or Kafka checkpoint configuration is changed.

## What was fixed

Previously the Streamlit form called `POST /stations/onboard`, holding an HTTP request open while Spark cleaned a large raw workbook, Open-Meteo supplied weather, CSVs were generated and the station was registered. Navigating away could abandon the browser-side HTTP request. Even if processing was still running, the page had no durable job ID or timeline.

Now the UI calls `POST /stations/onboard/jobs`. FastAPI first saves the source upload locally, creates a durable job record, and returns **HTTP 202 + job ID**. A backend worker runs the existing onboarding function independently of the Streamlit page. The existing serial automatic-training supervisor still queues/deploys models. Thus navigating from Stations to Home/Monitoring/Models does not cancel the background registration once the server has acknowledged the upload.

### Process milestones

- Raw file accepted by FastAPI
- Validate station ID, hierarchy, configuration and replay date
- Spark raw-data cleaning and validation (recent `logs/<station>_raw_spark.log` can be inspected from the dashboard)
- Weather download and feature alignment
- Prepare model-training and replay files
- Save station registry and hierarchy
- Create Kafka topics (or report when Kafka is offline)
- Queued/running/deployed/failed for each selected model (Demand Dense NN, Solar/Wind LSTM and Residual Autoencoder)
- Final **All selected models deployed (N/N)** notification, not merely "registered"

The full process table, stage timeline and latest backend update time are on the Stations page. A compact live progress notice appears on **every dashboard page**. Both refresh independently every two seconds and read status from FastAPI, not the Streamlit session.

## Installation (Windows)

1. Stop the FastAPI and Streamlit terminals. Do **not** reset station replays or delete Kafka topics.
2. Make a backup of the existing project.
3. Extract ZIP. Copy only the contents of `overlay/` over the matching project paths, or run `python preflight_and_apply.py "C:\\path\\to\\your\\project"` to check the source-file checksums and back up each file before applying.
4. Restart FastAPI (single Uvicorn worker) and Streamlit.
5. In **Stations**, upload one raw file, confirm weather locations, select replay date, and click **Process raw file and register station**.
6. Wait for **Upload accepted** before navigating away. Home and other pages show the in-progress job; returning to Stations restores the detailed timeline.

## New paths and endpoints

- Job status database: `data/state/onboarding_jobs/jobs.sqlite3` (created automatically)
- Temporary upload: `data/state/onboarding_jobs/<job-id>/customer_source.csv|xlsx|xlsm`; removed after **successful station registration**, since the original file is archived under that station
- `POST /stations/onboard/jobs` (multipart upload; accepts same fields as old form)
- `GET /stations/onboard/jobs?limit=10`
- `GET /stations/onboard/jobs/{job_id}`
- `GET /stations/onboard/jobs/{job_id}/spark-log`
- Existing synchronous `POST /stations/onboard` unchanged for old callers

## Important operating limits

- Navigation/browser refresh does not cancel a job **after FastAPI acknowledges the file upload**. While the original file is still uploading, leaving the page may interrupt the transfer.
- If **FastAPI itself** crashes during Spark/weather preprocessing, its job record is marked **Interrupted** after restart, rather than falsely marked successful. Check whether the station is registered; retry through the portal if needed. This patch does **not** automatically resume a partially completed Spark run.
- Automatic training queue state continues to be tracked from `config/stations.yaml`; training is marked complete only when all selected components are `ready`.
- Use **one Uvicorn worker** (the default when running `uvicorn backend.main:app`) because the lightweight background worker and Spark semaphore are process-local. Multiple API workers require a distributed job queue, which is intentionally outside the laptop demo scope.
- API/backend must stay running to process jobs; it is not a cloud/offline scheduler.
- Never claim weather or Kafka succeeded if the UI shows an error, pending topics, or a failed stage.

## Files

- `overlay/backend/onboarding_jobs.py` — durable job table, queue, worker, live model status
- `overlay/backend/station_api.py` — background upload and polling routes; fine-grained stage events
- `overlay/dashboard/pages/station_management.py` — nonblocking submission, progress table, log tail, timeline
- `overlay/dashboard/ui.py` — compact persistent registration banner in shared navigation
- `tests/test_onboarding_background_jobs.py` — mocked success, failure and backend-restart tests

## Verification

- `python -m py_compile` passed for changed application files.
- 20 focused tests passed for background jobs, raw upload registration, location acceptor and onboarding training.
- HTTP route registration/listing exercised using FastAPI TestClient.
- The full suite could not collect in this isolated environment because `pyspark`, `kafka-python`, and `streamlit` are not installed here. Validate the actual Spark/Windows/Kafka demo on your laptop.
