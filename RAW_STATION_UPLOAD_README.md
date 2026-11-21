# Raw station upload patch

This ZIP applies to the project inside `Performance upgrade(1).zip`. Stop FastAPI and Streamlit, then extract this patch into the `Performance upgrade` project folder where `backend`, `dashboard` and `src` already exist. Replace the included files and restart both services from that folder using your existing project environment. PySpark and a working Java installation are required. The batch importer reuses the application's Java configuration.

## Register a station

1. Open **Stations → Register a station or substation**.
2. Upload one UTF-8 CSV or Excel workbook. Put headers on the first row and keep observations in time order. For Excel, select a worksheet only if needed to identify a single data sheet.
3. Set the source time zone, interval, demand unit and a UTC replay start date. For the supplied 50 Hertz reference, use `Europe/Berlin`, 15 minutes and **Auto detect** for its timestamp format. Use `UTC` for naive timestamps that are already expressed in UTC. ISO timestamps with an explicit offset are interpreted by that offset.
4. Add representative weather locations in the form as `name, latitude, longitude`, one per line. The station can use the existing weather cache for offline replay, provided the cache covers its locations and date range. For a live demo with a new station, complete an online rehearsal before relying on cache-only registration.
5. Select the models and click **Process raw data and register station**. Follow the progress through Spark, weather, registration, Kafka topic setup and model deployment. After deployment, activate the station to start replay.

The CSV importer accepts commas, semicolons and tabs. Weather coordinates are entered in the portal. In **Optional import settings**, use `field=source header` to map an unfamiliar or ambiguous column. Extra source columns are ignored.

| Mapping field | Recognized header from the reference |
|---|---|
| `timestamp` | `Datum von` |
| `demand` | `Demand` |
| `solar_mwh` | `Photovoltaics [MWh]` |
| `solar_mw` | `Photovoltaics_installed_capacity [MW]` |
| `wind_mwh` | `Wind onshore [MWh]` |
| `wind_mw` | `Wind_onshore_installed_capacity [MW]` |

## What the data path does

- An isolated Spark batch job stages selected columns, resolves duplicate intervals, normalizes time to UTC and builds the expected interval grid. Source timestamp gaps above 20% fail validation. Spark fills missing demand and renewable generation from the latest available matching historical weekday and time, then matching daily time, then the last actual reading. Installed capacity is carried forward from the last available value. Leading intervals before a complete prior baseline are skipped and counted.
- FastAPI enriches the cleaned datasets with weather and writes model inputs. Rows before the replay date feed training and validation. Rows from the replay date feed the demo. The original file is archived with the station. Spark diagnostics appear in registration progress and worker output is available in `logs/<station>_raw_import_<id>.log`.
- Replay uses the existing station producer and streaming workers. The registration page uploads raw files, while existing API clients can continue to use the legacy workbook mode. No configuration files are included in this patch.

## 50 Hertz reference

Spark prepared 168,270 intervals, with zero timestamp gaps. It filled 7 Demand values and 9 Wind Energy values. The renewable capacity checks for the residual-load trainer flagged both Solar and Wind Energy. For example, 2,500.50 MWh over 15 minutes implies 10,002 MW against the reported Wind Energy capacity of 1,704 MW. Confirm renewable generation units, the coverage of the capacity series and that both sources describe the same station area. Use **Demand forecasting** alone for a demo of this file until the source issue is resolved.

Verification covered the 50 Hertz Excel reference, its CSV export, the 20% timestamp threshold, existing registration and replay tests, and HTTP onboarding with a valid synthetic CSV. The synthetic run exercised four model input files, the three replay feeds and the producer's CSV reader using test weather and a test Kafka topic setup.
