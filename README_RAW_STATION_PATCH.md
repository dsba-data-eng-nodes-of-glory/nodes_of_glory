# Nodes of Glory — Raw one-file Spark onboarding patch
**Base:** the uploaded `Performance upgrade(2).zip` (21 Nov 2026).
This is an overlay patch, NOT a replacement project or Cassandra migration.

## Install
1. Back up your existing project (especially `config/stations.yaml`, `data`, `models` and `checkpoints`).
2. Stop FastAPI and Streamlit. Do not reset or erase the registered stations.
3. Copy the contents of `overlay/` into the project root, preserving directory structure.
4. Use your Windows `electricity-spark` Conda environment with `pyspark`, `openpyxl`, `pandas`, `fastapi`, `streamlit`, `pyyaml` and configured Java 17. The raw cleaner launches a *separate* short-lived Spark job. Java home is read from `config/spark.yaml`.
5. Restart FastAPI and Streamlit. No changes to `config/stations.yaml`, Kafka topics or `config/spark.yaml` are required for existing stations.

## Customer workflow (no standard workbook)
Station Management > Register station:
* Enter station ID, display name, hierarchy path and UTC replay start date.
* Upload ONE raw CSV/XLSX/XLSM containing source timestamp, Demand, Solar Energy,
  Solar Installed Capacity, Wind Energy and Wind Installed Capacity. Irrelevant columns are ignored.
* Enter weather representative points in the form as `Name, latitude, longitude`,
  1–12 lines. Do NOT put weather columns in the raw file.
* `auto` timestamp zone assumes UTC for UTC-labeled source timestamps; otherwise
  Europe/Berlin. Choose `UTC` or `Europe/Berlin` explicitly if necessary.
* Select German/English numeric format. For uncommon headers, enter
  `canonical_key=Actual Source Header` in optional raw-column mapping.
  Canonical keys are `timestamp`, `demand`, `solar_mwh`, `solar_mw`,
  `wind_mwh`, `wind_mw`.
* Click **Process raw file and register station**. Spark processes and audits raw
  data. The backend builds individual training and replay CSVs and enriches them
  with weather. Existing auto-training and Kafka topic creation are unchanged.
  Activate only after selected models have deployed.

## Spark processing contract
The uploaded Spark RDD algorithm (with necessary fixes) performs:
* Source timestamp parsing and UTC normalization, duplicate reconciliation,
  ordering and optional cadence resampling.
* Source value casting, historical baseline / past-only gap repair, quality report
  and optional imputation/duplicate audits, while ignoring irrelevant columns.
* Quality gate **10% missing timestamp slots AND 10% missing original
  measurements PER Demand/Solar/Wind target**, checked before imputation.
  This follows the NEW attached 21 November script, not the older 20% rule.
* Registration runs Spark with `--causal-only`: a missing Wind Energy point
  cannot use a future observation. No retrospective wind bridge is used for
  training/replay. Rows with unfillable *leading/trailing* measurements are
  trimmed; interior missing measurements reject registration.
* Capacity mismatch and other source issues are reported by Spark. Inspect
  the quality report before trusting forecasting performance.

## Generated station artifacts
- Original upload: `data/raw/stations/<station>/<station>_source.csv` (or .xlsx)
- Spark cleaned data: `data/raw/stations/<station>/source_upload_cleaned.csv`
- Spark quality report: `data/raw/stations/<station>/source_upload_quality.json`
- Spark imputation audit: `data/raw/stations/<station>/source_upload_imputation_audit.csv`
- Backend station-scoped demand/solar/wind model-ready CSVs in the same folder
- Model checkpoints, operational results and Kafka checkpoint paths remain as configured.
- A full Spark processing log: `logs/<station>_raw_spark.log`

## Important safety / evaluation notes
- The stream replay begins at the chosen UTC date; no reading at or after
  that date is used for fitting. Existing offline train/validation/test
  split logic is unchanged.
- Causal imputation prevents future actuals from entering earlier replay
  readings. However, imputations are *estimated measurements*, not verified
  ground truth. For rigorous model scoring, filter or mask imputed targets
  using the imputation audit. The existing trainer does not yet automatically
  mask them in every evaluation metric.
- All six source fields are mandatory for this particular attached Spark
  processor even when you deselect a model. Never invent a missing target
  or installed capacity. Demand units must be chosen correctly.
- Weather downloads may still encounter Open-Meteo rate limiting and
  archived-forecast coverage gaps; this patch does NOT bypass provider limits.
- Registration preprocesses an entire source file before activation. Avoid
  running large raw registrations concurrently with a four-station live demo
  on an 8 GB Windows laptop.
- An already registered station and its YAML, checkpoints, models, offsets,
  and monitoring outputs are not modified by extracting this patch.
- The old `/stations/template` endpoint remains for backward API compatibility,
  but the customer-facing portal no longer requires or offers the template.

## Validation evidence
* Pure-Python staging: source with irrelevant columns, 15-minute UTC and
  Europe/Berlin timestamps, 10% quality gate.
* Spark worker reconstruction tested in-process with simulated typed executor
  rows; cannot replace a real Spark runtime test.
* `tests/test_raw_station_upload_patch.py` and
  `tests/test_raw_station_upload_endpoint.py`:
  seven parser/loader cases and one mocked registration replay split.
* Original legacy workbook registration and hierarchy tests also pass.
* **PySpark is unavailable in the test environment**, so real JVM + Spark
  processing, Open-Meteo weather fetching, automatic model training, and
  Windows Kafka streaming must be validated on your laptop.

## Manual verification commands
```
python -m pytest tests/test_raw_station_upload_patch.py tests/test_raw_station_upload_endpoint.py -q
python scripts/raw_spark_preprocessor.py --help
```
After installation, register a test station with a fresh ID. Inspect:
`logs/<station>_raw_spark.log`,
`data/raw/stations/<station>/source_upload_quality.json`,
and the saved training/replay files. Verify the first replay timestamp
and only then activate the station. Do not reset existing production stations.
