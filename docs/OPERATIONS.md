# Station data and operations

This guide consolidates useful instructions from the removed patch READMEs. The source is assembled already; applying old ZIP overlays or rerunning their installers is unnecessary.

## Station registration

Register only a new station; existing registered stations can be activated without uploading the same data again.

1. Set a unique ID, hierarchy, interval, source time zone, numeric format, demand units and UTC replay date.
2. Upload raw CSV/XLSX/XLSM with headers. The Spark importer expects timestamp, demand, solar energy, solar installed capacity, wind energy and wind installed capacity. Canonical mappings: `timestamp`, `demand`, `solar_mwh`, `solar_mw`, `wind_mwh`, `wind_mw`. Map ambiguous headers in the portal; extra columns are ignored. Energy and capacity units describe different quantities.
3. Search/confirm weather sampling places or manually enter 1–12 lines of `name, latitude, longitude`. These points represent the service area's weather; they do not create substation hierarchy nodes. Do not invent weather columns in the upload.
4. Submit background registration and wait for upload acknowledgement before navigating away. The backend must remain running.
5. Review and explicitly approve/cancel the Spark quality report. Even accepted data without issues needs acknowledgement. Hard rejections cannot be overridden. Approval enables weather, dataset/registry/topic setup and selected-model training; it does not guarantee reliable models.
6. Wait for all selected components to become deployed/ready. Inspect stage errors/logs, then activate replay.

Only the newest job is displayed; older jobs remain in the audit store and are not cancelled by the UI. Avoid multiple submissions requiring separate approvals. Backend restarts mark interrupted jobs rather than silently approving/resuming them. Unconfirmed reviews expire after one hour.

### Cleaning and quality

Spark normalizes timestamps to UTC, reconciles duplicates and creates the interval grid. Current source-quality limits are 10% missing timestamp slots and 10% missing original measurements per required target, checked before imputation. Registration uses past-only repairs. Unfillable leading/trailing intervals are trimmed; unresolved interior measurements reject registration. Inspect the uploaded file's actual audit.

Imputed values are estimates. The trainer does not automatically mask every imputed evaluation target, so rigorous scoring should identify them using the audit. Check units and area/capacity coverage when generation converted to MW exceeds stated installed capacity. Do not invent missing targets/capacities to pass registration.

Under `data/raw/stations/<station>/`, inspect the original `<station>_source.*`, `source_upload_cleaned.csv`, `source_upload_quality.json`, `source_upload_imputation_audit.csv` when produced, and generated model/replay CSVs. Logs: `logs/<station>_raw_spark.log`. Exact dataset paths are saved in `config/stations.yaml`.

Weather uses configured representative locations and historical/day-ahead caches. Geocoding alone does not download weather. Provider rate limits, date coverage and archive gaps can stop registration or reduce usable model windows.

### Background API

Use `/docs` for request fields. Main routes:

| Route | Purpose |
| --- | --- |
| `POST /stations/locations/resolve` | Find weather-location candidates |
| `POST /stations/onboard/jobs` | Upload/enqueue registration |
| `GET /stations/onboard/jobs/{job_id}` | Status, stage, quality review |
| `GET /stations/onboard/jobs/{job_id}/spark-log` | Inspect Spark output |
| `POST /stations/onboard/jobs/{job_id}/quality-decision` | Submit `{"approved": true}` or `{"approved": false}` |

Decisions are single-use. New raw uploads must use background registration; direct raw `/stations/onboard` calls cannot bypass approval. Legacy-template API compatibility is separate. Job database: `data/state/onboarding_jobs/jobs.sqlite3`.

## Kafka replay and recovery

CSV producer → raw Kafka topic → Spark → featured Kafka topic → predictor → configured storage → FastAPI/dashboard. Spark writes featured and DLQ records from one `foreachBatch` query; predictor validation can also publish invalid records to the DLQ. Forecast outputs are persisted to storage; the dashboard does not consume Kafka directly.

The producer saves the last Kafka-acknowledged source timestamp in `data/state/<station>/producer_progress.json`. Ordinary restarts resume after it. A changed source missing that timestamp fails validation. Per-station locks prevent duplicate local producers. Keep offsets, source identity, checkpoints and outputs consistent.

Cross-sink delivery can be at least once; timestamp/forecast-key deduplication handles retries. **Stop + reset** deliberately clears station Kafka records, results, Spark checkpoints and the producer cursor. Reserve it for an intentional clean replay.

Three clocks differ: the observation interval (e.g. 15 minutes/row), producer delay/component `forecast_every_steps`, and UI polling (often two seconds). A two-second refresh does not guarantee two-second model inference. Kafka can buffer lagging work; station configuration sets actual cadence.

## Monitoring and troubleshooting

Historical actuals are cached per source signature and API worker; the first call can be slower. Live actuals take precedence at duplicate timestamps. Changed files invalidate the cache.

Renewable reads with `defer_heavy=true` can return `pending: true` while history loads. Residual reads with `cached_only=true` fetch predictor-saved assessments instead of executing the autoencoder on every GET. Bounded lookback can show an earlier compatible assessment with its age. Anomaly bands overlay a demand forecast only when origins match. Changed thresholds require a newly compatible saved assessment; GET does not rescore old records.

Hierarchy charts aggregate descendants at matching timestamps. A stopped/lagging child can hold the parent at the last common interval. Inspect reporting details and the partial-coverage legend for missing coverage. Residual load requires aligned demand, solar and wind values.

Check API health, Pipeline Control, worker logs, weather coverage, Kafka readiness and forecast origin. See `scripts/diagnose_monitoring_api.py --help` and `scripts/benchmark_actuals_api.py --help`. Model Management separates candidate training, comparison and deployment. Retain model checkpoint provenance files alongside artifacts.
