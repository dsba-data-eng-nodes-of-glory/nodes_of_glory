# Local setup

## Current workspace and services

```text
/Users/savts/Desktop/Study/DE at scale/Project/nodes_of_glory_workspace
```

Checkout: `dsba-data-eng-nodes-of-glory/nodes_of_glory`, branch `main`. Launch baseline: `de2f0cbc1528716d5cd75766b70747f888d9fd32`. The RF port is a subsequent local source change.

| Service | Current address |
| --- | --- |
| Dashboard | http://127.0.0.1:8509/ |
| FastAPI / API docs | http://127.0.0.1:8007 / http://127.0.0.1:8007/docs |
| Kafka | 127.0.0.1:9092 |
| Predictor metrics setting | Port 9302, served when its worker runs |

Amprion and Tennet were registered at setup and left stopped. Storage is **CSV**; Cassandra was not activated in this workspace.

## Changes made to launch on this Mac

The initial checkout needed no application-source fixes. Local environment/runtime changes were:

1. Created a Python 3.11 virtual environment reusing compatible installed dependencies.
2. Selected Java 17, added its `bin` to `PATH`, and set Spark's local IP to `127.0.0.1`.
3. Selected free API/dashboard ports 8007/8509 and set `ELECTRICITY_API_URL` accordingly.
4. Reused cached Spark/Kafka connector JARs and an existing Torch OpenMP library for XGBoost.
5. Isolated Kafka with namespace/group `nodes-of-glory-20261121`; raw/featured topics are `<namespace>-<station>-raw` and `<namespace>-<station>-featured`, with `<namespace>-dlq`. Spark defaults match the Amprion topics.
6. Selected CSV storage and metrics port 9302; disabled/stopped stations until explicit activation.
7. Parked imported runtime state and Spark checkpoints to avoid resuming paths/offsets copied from another computer. Existing weather SQLite caches were copied back. Source datasets and model artifacts were retained.

Original YAMLs: `.runtime/session/config_original/`. Imported state: `.runtime/session/imported_data_state/`. Imported checkpoints: `.runtime/session/imported_checkpoints/`. These are local recovery copies, not code dependencies. Upstream tracks runtime files, so Git can show many state/checkpoint deletions from setup. Review these separately from RF source/docs; do not stage everything indiscriminately.

### Environment dependencies

Current Python is 3.11.18. Verified local packages: PySpark 3.5.6, pandas 2.3.3, NumPy 2.4.6, scikit-learn 1.7.2, Torch 2.15.0, XGBoost 3.2.0, Streamlit 1.67.0, FastAPI 0.162.2, kafka-python 3.0.13. This inventory is not a lockfile.

The `.venv` was created from `/Users/savts/miniconda3/envs/nodes-of-glory/bin/python`. Its `.venv/lib/python3.11/site-packages/local_shared_dependencies.pth` references, in order:

```text
/Users/savts/Desktop/Study/DE at scale/Project/performance-upgrade/.venv/lib/python3.11/site-packages
/Users/savts/Desktop/Study/DE at scale/Project/demo-repository/.venv/lib/python3.11/site-packages
```

**The current environment depends on those folders.** Create an independent environment before deleting either older workspace.

## Launch the existing setup

Only run this when both ports are free; it refuses duplicate listeners:

```bash
cd "/Users/savts/Desktop/Study/DE at scale/Project/nodes_of_glory_workspace"
.venv/bin/python .runtime/session/launch.py
```

The local helper starts one FastAPI worker and Streamlit, records PIDs/commands in `.runtime/session/services.json`, and logs to `.runtime/session/backend.log` and `dashboard.log`. It does not activate stations. This helper is a local session file, not a tracked install script.

### Manual launch alternative

In **each** of two terminals, run the following environment setup:

```bash
cd "/Users/savts/Desktop/Study/DE at scale/Project/nodes_of_glory_workspace"
source .venv/bin/activate
export PYTHONPATH="$PWD"
export PYTHONDONTWRITEBYTECODE=1
export JAVA_HOME="/Users/savts/miniconda3/envs/nodes-of-glory/lib/jvm"
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_LOCAL_IP=127.0.0.1
export ELECTRICITY_API_URL=http://127.0.0.1:8007
export STORAGE_BACKEND=csv
export OMP_NUM_THREADS=2
export OPENBLAS_NUM_THREADS=2
export MKL_NUM_THREADS=2
export NUMEXPR_NUM_THREADS=2
export DYLD_LIBRARY_PATH="/Users/savts/Desktop/Study/DE at scale/Project/demo-repository/.venv/lib/python3.11/site-packages/torch/lib"
export ELECTRICITY_SPARK_KAFKA_JARS="$(python -c "from pathlib import Path; print(','.join(map(str, sorted(Path('/Users/savts/Desktop/Study/DE at scale/Project/demo-repository/.runtime/ivy/jars').glob('*.jar')))))")"
```

Terminal 1:

```bash
python -m uvicorn backend.main:app --host 127.0.0.1 --port 8007 --workers 1
```

Terminal 2:

```bash
python -m streamlit run dashboard/app.py --server.address 127.0.0.1 --server.port 8509 --server.headless true --browser.gatherUsageStats false
```

One API worker is required by the process-local onboarding queue/Spark semaphore. Set variables before starting the API so child workers inherit them. The Windows `java_home` shipped in `config/spark.yaml` is not the Java selection for this macOS launch.

## Independent setup on another computer

A fresh installation was not performed as part of this port. Prerequisites are Python 3.11, Java 17, a reachable Kafka broker and sufficient disk/RAM. Downloads, uncached Spark connectors, geocoding and weather calls need network access.

From your checkout, create a separate environment with a Python 3.11 executable:

```bash
python3.11 -m venv .venv-clean
source .venv-clean/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt "pyspark==3.5.6"
python -m pip install pytest
```

Select your own Java 17 path in `JAVA_HOME`, add its `bin` to `PATH`, and verify `java -version`. On macOS, `/usr/libexec/java_home -v 17` can find a registered JDK; Conda JDKs may require an explicit path. On Windows use `.venv-clean\Scripts\activate` and Windows environment-variable syntax.

Review station file paths, Kafka broker/group, Spark topic/checkpoint settings and storage backend in `config/`. Give independent replays separate topics and recovery state; do not combine unrelated offsets. Do not copy this Mac's `.pth`, OpenMP or JAR paths. If XGBoost reports a missing OpenMP library, provide a compatible platform runtime. Leave `ELECTRICITY_SPARK_KAFKA_JARS` unset for configured connector resolution, or supply JARs compatible with the installed Spark/Scala versions.

Start Kafka before activating a station. `docker-compose.scale.yml` is a multi-node Kafka/Cassandra deployment using different exposed Kafka ports and cluster config examples; it is not the current single-broker 9092 setup.

Start services using the foreground commands above with your environment and the portable variables `PYTHONPATH`, `SPARK_LOCAL_IP`, `ELECTRICITY_API_URL`, `STORAGE_BACKEND`. Keep the dashboard API URL consistent with the backend port. For VS Code notebooks, select the same Python environment as the kernel.

## Health and troubleshooting

```bash
curl --max-time 10 http://127.0.0.1:8007/health/live
curl --max-time 10 http://127.0.0.1:8007/health/ready
curl --max-time 10 http://127.0.0.1:8509/_stcore/health
```

- API readiness reports Kafka/storage separately. A visible dashboard does not prove station workers are running. Check Pipeline Control and logs after activation.
- The first historical-actuals request can exceed the UI timeout while its cache builds; later reads reuse prepared history. Inspect logs and `python scripts/benchmark_actuals_api.py --help` before resetting state.
- Renewable `pending: true` can mean a warming history cache; residual `pending: true` can mean the predictor has not saved a compatible assessment. See [operations](OPERATIONS.md).
- CSV mode needs no Cassandra. Enabling Cassandra and migrating data are separate operations: review configuration, schema and migration scripts before switching backends.
- For a normal restart, stop station pipelines in Pipeline Control, stop the services you started, restart from the same checkout/environment and activate as needed. Do not clear topics/cursors/outputs for an ordinary restart.
- A new RF candidate receives the additional input. Existing fitted artifacts are not changed by updating source code.
