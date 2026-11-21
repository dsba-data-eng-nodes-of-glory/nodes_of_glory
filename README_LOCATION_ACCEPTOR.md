# Nodes of Glory — Station Location Acceptor

**Compatibility:** This is an incremental overlay for the uploaded **Performance upgrade(2).zip** **with** the previously supplied `Nodes_of_Glory_Raw_One_File_Spark_Patch_V2.zip` applied. Do not combine this with a different version without comparing the affected files.

## Installation (Windows)
1. Back up `backend/station_api.py` and `dashboard/pages/station_management.py`.
2. Stop FastAPI and Streamlit (existing Kafka/Spark queries do not need a reset).
3. Extract this ZIP and copy the contents of its `overlay` folder into your project root, retaining folder structure.
4. Restart FastAPI and Streamlit. Existing `config/stations.yaml`, stations, model versions, Kafka topics, checkpoints and saved results are untouched.

## Customer registration: raw file + location acceptor
* Enter station ID, display name, hierarchy and replay start date as before.
* Under **Station service area & weather sampling locations**, choose **Search by place name** (default).
* Enter **one city/town or postcode per line** (up to 12); add country or region, e.g. `Dortmund, Germany` and `Essen, Germany`.
* Click **Find coordinates**. The backend queries Open-Meteo's geocoding API. Review the candidates displayed under each place, choose the correct candidate and press **Confirm selected locations**.
* The portal displays a map and the selected coordinates. Registration can now continue with **one raw CSV/XLSX/XLSM file**. The raw file needs no locations or weather columns.
* If geocoding is unavailable, choose **Enter coordinates manually** and enter lines as `Name, latitude, longitude`. Both paths use the same strict location validation.
* On **Process raw file and register station**, the already-confirmed points are sent with the raw file to the existing FastAPI onboarding endpoint. Backend registration saves them to `config/stations.yaml` under the new station's `weather.sites`, then the existing historical and forecast weather pipeline takes over.

## Important distinctions
* A **station's hierarchy** (country/state/station/substation) is configured separately. Geocoded places are **weather sampling coordinates for the service area**, not new stations or substation hierarchy nodes.
* An operator must confirm ambiguous place names. No automatic first-match selection is used when registering.
* Location lookup **does not download weather**; weather fetching is performed by the existing registration/backend weather service after the raw data passes Spark processing. Weather requests may still be rate-limited.
* A large transmission area may require multiple representative sampling locations. The selected places are treated as equal-weight samples by the existing weather service.
* This patch doesn't modify the Spark raw-data cleaning logic, training/replay split, model algorithms, or Kafka configuration.

## Files
- `overlay/dashboard/pages/station_management.py` — location input, candidate selection, map, manual fallback
- `overlay/backend/station_api.py` — `POST /stations/locations/resolve`
- `overlay/src/ingestion/station_location_lookup.py` — validation and Open-Meteo geocoding
- `overlay/tests/test_station_location_acceptor.py` — offline mocked tests

## Verification
In the extracted `raw_station_patch_work` copy, **16 tests passed**:
`pytest -q tests/test_station_location_acceptor.py tests/test_raw_station_upload_patch.py tests/test_raw_station_upload_endpoint.py`

These tests mock Open-Meteo and the Spark/weather registration path. Live geocoding/large Spark jobs/Windows Kafka execution are not verified in this environment. For the live demo, confirm the resolved locations and run the registration once before presentation day to cache historical weather.
