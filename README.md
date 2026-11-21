# Nodes of Glory

Electricity demand and renewable-generation forecasting, residual-load spike detection, and station pipeline monitoring.

Repository: <https://github.com/dsba-data-eng-nodes-of-glory/nodes_of_glory>

## Start here

- [Local setup and changes for this Mac](docs/LOCAL_SETUP.md): environment, launch commands, ports and troubleshooting.
- [Station data and operations](docs/OPERATIONS.md): inputs, Spark cleaning, quality approval, replay and monitoring.
- [Random Forest improvement](docs/random_forest_day_only.md): previous-day feature, compatibility and earlier validation evidence.

Current dashboard: <http://127.0.0.1:8509/>. API documentation: <http://127.0.0.1:8007/docs>. These addresses work while the services run on this computer.

## Project workflow

1. **Register data.** Upload a station CSV/Excel file and specify its time zone, interval, units, replay date and weather sampling locations.
2. **Prepare data with Spark.** Normalize timestamps to UTC, reconcile duplicates, construct the interval grid, apply past-only repairs and write quality diagnostics.
3. **Review and enrich.** Approve accepted source data in the dashboard. The backend aligns weather and prepares station-specific training/replay files.
4. **Train and evaluate.** Fit candidates using earlier observations and chronological validation; replay-configured stations also have a held-out test partition where supported by the component trainer.
5. **Stream and predict.** A CSV producer publishes to the station's raw Kafka topic. Spark consumes it and publishes featured events. The predictor consumes featured events, builds model inputs, forecasts and persists results. Invalid events can go to the dead-letter topic.
6. **Monitor.** FastAPI serves stored observations, forecasts and residual-load assessments to Streamlit. Pipeline controls start/stop workers; model controls compare and deploy candidate versions.

The local setup replays historical files, rather than connecting to a live grid feed. At a 15-minute interval, the standard demand contract is seven days of history (672 observations) and the next 24 hours (96 targets). Other supported intervals derive their step counts from these durations.

## Models

| Component | Available approaches | Output |
| --- | --- | --- |
| Demand forecasting | Dense NN, LSTM, Random Forest, GBT, XGBoost | Demand forecast in MW |
| Solar forecasting | Dense NN, LSTM, Random Forest, GBT, XGBoost | Solar-generation forecast |
| Wind forecasting | Dense NN, LSTM, Random Forest, GBT, XGBoost | Wind-generation forecast |
| Residual-load detection | Dense autoencoder with statistical/ramp screening | Anomaly assessments and alerts |

Forecast regressors use supervised learning: the label is observed demand or generation at the target timestamp. Autoencoder reconstruction is unsupervised; the residual component is not a supervised Random Forest spike classifier. Residual load is demand minus solar and wind generation.

**Random Forest port:** new *demand* RF training uses the original tabular inputs plus `demand_target_previous_day`: 37 inputs at 15-minute resolution. The extra target-specific weekly input is omitted; original weekly lags remain. Other model/component inputs are unchanged. Feature schema `electricity_tabular_v3` identifies this layout and is separate from experiment version labels such as v4. Saved models retain their feature layouts until retrained and explicitly deployed.

## Data and configuration

| Location | Purpose |
| --- | --- |
| `config/stations.yaml` | Source paths, hierarchy, topics, weather, deployed components |
| `config/model.yaml` | History/horizon durations and inference settings |
| `config/algorithm_configs.yaml` | Algorithm parameters and sampling budgets |
| `config/kafka.yaml`, `config/spark.yaml` | Broker, processing and checkpoint settings |
| `config/storage.yaml` | CSV/Cassandra backend, quality and observability |
| `data/raw/stations/<station>/` | Uploaded sources, Spark audits and generated datasets |
| `data/training/` | Training data for stations using this older layout |
| `data/processed/stations/<station>/` | CSV-mode outputs and local assessment stores |
| `data/state/`, `checkpoints/` | Recovery cursors, job state and Spark progress |
| `models/<station>/experiments/<id>/` | Candidate artifacts and metadata |
| `logs/` | Worker and processing logs |

Station-configured file paths are the source of truth; multiple layouts exist. Dataset attribution to OPSD remains a **placeholder pending verification of the actual station files**. Confirm provenance, units, coverage and licensing before publishing results/data.

## Evaluate a new demand Random Forest

1. In **Model Management → Configure & Retrain**, select the station, Demand component and Random Forest.
2. Confirm the training source, parameters and evaluation budget; submit a candidate.
3. Inspect its `metadata.json`: expect schema `electricity_tabular_v3`, `demand_target_previous_day` in `feature_names`, and no `demand_target_previous_week`.
4. Compare MAE, RMSE, WAPE, training time and latency with an original RF on the same source, split and sampled forecast origins. Lower error is better; WAPE is not classification accuracy.
5. Deploy the selected candidate in Model Management and restart the affected station pipeline if required by deployment status. Training alone does not replace an active model.

This port includes no new production training or deployment. Earlier feature-selection results are in the [RF note](docs/random_forest_day_only.md).

## Focused verification

From the repository root, using the project environment:

```bash
python -m pytest -q tests/test_target_demand_features.py tests/test_tabular_algorithm_scope.py tests/test_train_origin_sampling.py tests/test_dynamic_interval.py
```

Coverage includes alignment, future-label isolation, RF-only routing, saved-model compatibility, chronological splits, weather and NN blending. This is not a full Kafka/Spark rehearsal. Historical ZIP-patch READMEs have been consolidated into the guides above; model checkpoint provenance documentation is retained.
