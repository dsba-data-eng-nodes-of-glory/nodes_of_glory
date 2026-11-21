from __future__ import annotations

# Allow Streamlit multipage navigation to import project modules even when
# launched from the dashboard directory or a different working directory.
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import requests
import pandas as pd
import streamlit as st

from src.ingestion.station_location_lookup import confirmed_location_lines, LocationLookupError
from src.ingestion.raw_station_loader import parse_weather_locations

try:
    from dashboard.ui import setup_page, section_title, render_status_strip
    from dashboard.station_tree import group_paths
except ModuleNotFoundError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from dashboard.ui import setup_page, section_title, render_status_strip
    from dashboard.station_tree import group_paths

from src.storage.station_hierarchy import station_label

def rerun_page():
    rerun = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
    if rerun:
        rerun()

api = setup_page("Station Management", "stations")
st.title("Station Management")



def api_get(path: str):
    response = requests.get(f"{api}{path}", timeout=15)
    response.raise_for_status()
    return response.json()


def api_post(path: str, *, json=None, data=None, files=None, timeout=60):
    response = requests.post(f"{api}{path}", json=json, data=data, files=files, timeout=timeout)
    response.raise_for_status()
    return response.json()


def api_delete(path: str, *, json=None, timeout=240):
    response = requests.delete(f"{api}{path}", json=json, timeout=timeout)
    response.raise_for_status()
    return response.json()


try:
    registry = api_get("/stations")
except requests.RequestException as exc:
    st.error(f"Cannot reach FastAPI at {api}: {exc}")
    st.stop()

stations = registry.get("stations", {})

# ---------------------------------------------------------------------
# Register new station
# ---------------------------------------------------------------------
st.subheader("Register a station or substation")
try:
    parent_choices = group_paths(api_get("/stations/hierarchy").get("nodes", []))
except requests.RequestException:
    parent_choices = {}
parent_choice = st.selectbox(
    "Reuse an existing parent branch",
    [None, *sorted(parent_choices, key=lambda key: parent_choices[key])],
    format_func=lambda key: "Enter a new hierarchy or register a standalone station" if key is None else
    " / ".join(name for _, name in parent_choices[key]),
)
path_prefix = "\n".join(f"{level}: {name}" for level, name in parent_choices.get(parent_choice, []))

with st.expander("Raw file onboarding — no template required", expanded=False):
    st.markdown(
        "Upload one CSV or Excel file containing UTC/local timestamps, Demand, "
        "Solar generation and capacity, and Wind Energy generation and capacity. "
        "Extra columns are ignored. Spark detects the usual source header aliases, "
        "repairs allowable gaps from earlier readings, and produces model-ready "
        "training and replay datasets. Enter weather coordinates below; weather "
        "is downloaded by the backend, not supplied in the raw file. "
        "The attached Spark processor rejects more than 10% missing timestamp slots "
        "or more than 10% missing target readings per Demand/Solar/Wind column."
    )
model_labels = {
    "demand_forecast": "Demand forecasting",
    "solar_forecast": "Solar forecasting",
    "wind_forecast": "Wind Energy forecasting",
    "residual_anomaly": "Residual Load anomaly autoencoder",
}
stream_input = st.selectbox(
    "Stream input", ["csv_replay", "kafka"],
    format_func=lambda value: "Historical CSV replay" if value == "csv_replay" else "Live Kafka feed",
)
# ---------------------------------------------------------------------
# Customer service-area location acceptor (independent of the raw file)
# ---------------------------------------------------------------------
st.markdown("**Station service area & weather sampling locations**")
st.caption(
    "Enter the cities/places served by this station. The portal finds the coordinates; "
    "review them before registering. These are weather sampling points, not hierarchy nodes. "
    "For large coverage areas, add representative places (up to 12)."
)
location_entry_mode = st.radio(
    "Location entry method",
    ["Search by place name", "Enter coordinates manually"],
    horizontal=True,
    key="station_weather_location_mode",
)
weather_locations = ""
if location_entry_mode == "Search by place name":
    location_names = st.text_area(
        "Locations served (one place per line)",
        key="station_service_area_places",
        placeholder="Dortmund, Germany\nEssen, Germany\nCologne, Germany",
        help="Enter 1–12 cities, towns or postcodes. Add the country or state after a comma for clarity.",
    ).strip()
    place_queries = [line.strip() for line in location_names.splitlines() if line.strip()]
    if st.button("Find coordinates", key="resolve_station_locations"):
        st.session_state.pop("station_location_confirmation", None)
        st.session_state.pop("station_location_lookup", None)
        if not 1 <= len(place_queries) <= 12:
            st.error("Enter between 1 and 12 place names before looking up coordinates.")
        else:
            try:
                result = api_post(
                    "/stations/locations/resolve",
                    json={"locations": place_queries},
                    timeout=(5, 120),
                )
                st.session_state["station_location_lookup"] = {
                    "source": location_names,
                    "locations": result["locations"],
                }
            except requests.HTTPError as exc:
                try:
                    detail = exc.response.json().get("detail", exc.response.text)
                except (ValueError, AttributeError):
                    detail = str(exc)
                st.error(f"Could not resolve locations: {detail}")
            except requests.RequestException as exc:
                st.error(f"Location lookup is unavailable: {exc}. Use manual coordinates if necessary.")

    lookup = st.session_state.get("station_location_lookup") or {}
    if lookup.get("source") == location_names and lookup.get("locations"):
        st.caption("Choose the correct location when names are ambiguous, then confirm.")
        selections = []
        for index, match in enumerate(lookup["locations"]):
            candidates = match["candidates"]
            selected_idx = st.selectbox(
                f"{match['query']} — matching locations",
                options=list(range(len(candidates))),
                format_func=lambda idx, opts=candidates: (
                    f"{opts[idx]['display']} "
                    f"({opts[idx]['latitude']:.5f}, {opts[idx]['longitude']:.5f})"
                ),
                key=f"station_location_pick_{index}_{match['query']}",
            )
            selections.append(candidates[selected_idx])
        selected_signature = [(row["location"], row["latitude"], row["longitude"]) for row in selections]
        if st.button("Confirm selected locations", type="primary", key="confirm_station_locations"):
            try:
                canonical = confirmed_location_lines(selections)
                parse_weather_locations(canonical)  # same rules used by /stations/onboard
                st.session_state["station_location_confirmation"] = {
                    "source": location_names,
                    "signature": selected_signature,
                    "canonical": canonical,
                }
            except (LocationLookupError, ValueError) as exc:
                st.error(str(exc))
        confirmed = st.session_state.get("station_location_confirmation") or {}
        if (confirmed.get("source") == location_names
                and confirmed.get("signature") == selected_signature):
            weather_locations = confirmed["canonical"]
            st.success(f"{len(selections)} locations confirmed. Weather coordinates are ready.")
            st.dataframe([
                {"Location": item["location"], "Latitude": item["latitude"], "Longitude": item["longitude"]}
                for item in selections
            ], hide_index=True, use_container_width=True)
            st.map(pd.DataFrame({"lat": [row["latitude"] for row in selections],
                                 "lon": [row["longitude"] for row in selections]}))
        else:
            st.info("Confirm the locations above before registering the station.")
    elif location_names:
        st.info("Click Find coordinates, select the correct places, and confirm before registration.")
else:
    weather_locations = st.text_area(
        "Weather locations: Name, latitude, longitude (one per line)",
        key="station_service_area_coordinates",
        placeholder="Dortmund, 51.5136, 7.4653\nEssen, 51.4556, 7.0116",
        help="Use when location search is unavailable or you have surveyed coordinates. "
             "Add 1–12 points for the station service area.",
    ).strip()
    if weather_locations:
        try:
            preview_sites = parse_weather_locations(weather_locations)["sites"]
            st.caption(f"{len(preview_sites)} valid locations will be saved with this station.")
            st.map(pd.DataFrame({"lat": [row["latitude"] for row in preview_sites],
                                 "lon": [row["longitude"] for row in preview_sites]}))
        except ValueError as exc:
            st.warning(f"Location input needs correction: {exc}")

with st.form("station_onboarding_form", clear_on_submit=False):
    c1, c2 = st.columns(2)
    with c1:
        station_id = st.text_input("Station or substation ID", placeholder="nrw-substation-01").strip().lower()
        display_name = st.text_input("Display name", placeholder="Substation 01")
        hierarchy_path = st.text_area(
            "Parent hierarchy (top to bottom)", value=path_prefix, key=f"parent_path_{parent_choice}",
            placeholder="Country: Germany\nState: North Rhine-Westphalia\nStation: Station A",
            help="Enter one parent per line as Level: Name. Add extra levels as needed. Leave blank for a standalone source station.",
        ).strip()
        replay_start_date = None
        if stream_input == "csv_replay":
            replay_start_date = st.date_input(
                "Replay start date (UTC)", value=None,
                help="Replay starts at 00:00 UTC. Earlier rows are used for model development. Choose the same date for stations you want to compare.",
            )
        uploaded = st.file_uploader(
            "Customer raw data (single CSV or Excel file)",
            type=["csv", "xlsx", "xlsm"], key="station_workbook_upload",
            help="No fixed template, no separate training/streaming sheets; extra columns are ignored.",
        )
    with c2:
        st.markdown("**Timestamp configuration**")
        format_options = {
            "Auto detect / ISO 8601 (recommended)": "auto",
            "DD-MM-YYYY HH:MM": "%d-%m-%Y %H:%M",
            "DD-MM-YYYY HH:MM:SS": "%d-%m-%Y %H:%M:%S",
            "DD.MM.YYYY HH:MM": "%d.%m.%Y %H:%M",
            "DD/MM/YYYY HH:MM": "%d/%m/%Y %H:%M",
            "YYYY-MM-DD HH:MM": "%Y-%m-%d %H:%M",
            "Custom format": "__custom__",
        }
        timestamp_format = format_options[st.selectbox("Timestamp format", list(format_options))]
        if timestamp_format == "__custom__":
            timestamp_format = st.text_input("Python datetime format", value="%Y-%m-%d %H:%M:%S").strip()
        source_timezone = st.text_input(
            "Timezone for timestamps without an offset", value="auto",
            help="auto: UTC headers use UTC; other timestamps use Europe/Berlin. You may enter UTC or Europe/Berlin explicitly.",
        ).strip()
        st.caption("Weather locations are selected and verified in the Service Area section above.")
        numeric_locale = st.selectbox(
            "Raw number format", ["de", "en"],
            format_func=lambda code: "German / EU (1.234,56)" if code == "de" else "English / international (1,234.56)",
        )
        with st.expander("Optional raw-column mapping", expanded=False):
            raw_column_overrides = st.text_area(
                "Source column overrides",
                placeholder="timestamp=Measured at\ndemand=Grid demand [MWh]\nwind_mw=Wind capacity (MW)",
                help="One canonical_key=actual source heading per line. Keys: timestamp, demand, "
                     "solar_mwh, solar_mw, wind_mwh, wind_mw. Leave blank for automatic detection.",
            )
            raw_sheet_name = st.text_input(
                "Excel worksheet name (optional)",
                value="", help="Leave empty to use 50 hertz sheet if present, otherwise first sheet.",
            )
        interval_minutes = int(st.number_input(
            "Source observation interval (minutes)", min_value=5, max_value=1440, value=15,
            help="The current solar/wind and anomaly trainers require 15-minute observations.",
        ))
        residual_demand_unit = st.selectbox(
            "Demand input units",
            options=["mwh_per_interval", "mw"],
            format_func=lambda value: "MWh per source interval" if value == "mwh_per_interval" else "MW (average power)",
            help="Solar and Wind Energy are provided as MWh per 15-minute interval. MW demand is converted to MWh before subtraction.",
        )
        if 10080 % interval_minutes != 0 or 1440 % interval_minutes != 0:
            st.warning("Interval must divide both the 7-day lookback and 24-hour horizon.")
        elif interval_minutes != 15:
            st.warning("Solar, wind and anomaly components currently support only 15-minute data; deselect them for other intervals.")
        else:
            st.caption(f"7-day history: {10080 // interval_minutes} rows · 24-hour horizon: {1440 // interval_minutes} rows")
    st.markdown("**Select models to train automatically**")
    st.caption("Dense NN for Demand, LSTM for Solar and Wind Energy, and Dense Autoencoder for residual-load anomalies.")
    selected_models = []
    mcols = st.columns(4)
    for idx, (model_id, label) in enumerate(model_labels.items()):
        with mcols[idx]:
            if st.checkbox(label, value=True, key=f"onboard_{model_id}"):
                selected_models.append(model_id)
    st.caption("Residual-load detection requires all three Demand, Solar and Wind Energy datasets, including installed-capacity columns.")
    st.markdown("**Optional Kafka topic names**")
    topic_col1, topic_col2 = st.columns(2)
    with topic_col1:
        raw_topic = st.text_input("Raw topic (automatic if empty)", placeholder="tennet-raw")
    with topic_col2:
        featured_topic = st.text_input("Featured topic (automatic if empty)", placeholder="tennet-featured")
    submitted = st.form_submit_button("Validate raw file and review quality", type="primary")

if submitted:
    if not station_id or uploaded is None:
        st.error("Enter a station ID and upload a raw CSV/Excel file.")
    elif stream_input == "csv_replay" and replay_start_date is None:
        st.error("Select a replay start date.")
    elif not weather_locations.strip():
        st.error("Confirm service-area locations above, or enter valid manual coordinates.")
    elif not selected_models:
        st.error("Select at least one model component.")
    elif "residual_anomaly" in selected_models and not {"demand_forecast", "solar_forecast", "wind_forecast"}.issubset(selected_models):
        st.error("Residual-load anomaly detection requires Demand, Solar and Wind Energy forecasting.")
    elif interval_minutes != 15 and any(m != "demand_forecast" for m in selected_models):
        st.error("Solar, wind and anomaly training currently require 15-minute data.")
    else:
        st.info("Uploading your file to FastAPI. Once accepted, registration continues independently "
                "of this page. You can navigate to other pages while Spark and model training run.")
        try:
            result = api_post(
                "/stations/onboard/jobs",
                data={
                    "station_id": station_id,
                    "display_name": display_name or station_id.upper(),
                    "hierarchy_path": hierarchy_path,
                    **({"replay_start_date": replay_start_date.isoformat()} if replay_start_date else {}),
                    "stream_input": stream_input,
                    "timestamp_format": timestamp_format,
                    "source_timezone": source_timezone or "UTC",
                    "interval_minutes": interval_minutes,
                    "selected_models": ",".join(selected_models),
                    "residual_demand_unit": residual_demand_unit,
                    "raw_topic": raw_topic.strip(),
                    "featured_topic": featured_topic.strip(),
                    "weather_locations": weather_locations,
                    "numeric_locale": numeric_locale,
                    "raw_sheet_name": raw_sheet_name,
                    "raw_column_overrides": raw_column_overrides,
                    "upload_mode": "raw",
                },
                files={"file": (uploaded.name, uploaded.getvalue(),
                          "text/csv" if uploaded.name.lower().endswith(".csv") else
                          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                timeout=(20, 180),
            )
            st.session_state["onboarding_job_id"] = result["job_id"]
            st.session_state["registered_station_selection"] = result["station"]
            st.success(f"Upload accepted for {result['station']}. Spark validation has started. "
                       "Training will begin only after you approve the quality review.")
            rerun_page()
        except requests.HTTPError as exc:
            try:
                detail = exc.response.json().get("detail", exc.response.text)
            except (ValueError, AttributeError):
                detail = str(exc)
            st.error(f"Station registration failed: {detail}")
        except requests.RequestException as exc:
            st.error(f"Station registration failed: {exc}")

# The customer must explicitly approve a Spark-accepted dataset before
# weather download, station registration and model deployment begin.
def _render_quality_review(job: dict) -> None:
    review = job.get("quality_review") or {}
    if job.get("status") != "awaiting_confirmation" or not review:
        st.info("This registration is not currently awaiting confirmation.")
        return
    st.markdown(f"**Station:** {job['station']} · **Validated rows:** {review.get('rows', 0):,}")
    if review.get("training_risk"):
        st.warning("⚠️ **Potential model-quality compromise.** " + review.get("summary", ""))
        if not review.get("training_ready", False):
            st.error("Spark accepted the source, but marked it NOT training-ready. "
                     "Continuing does not guarantee usable model performance.")
    else:
        st.success("Spark accepted the dataset without quality warnings.")
    st.caption("The Spark hard-rejection rules cannot be overridden. This review applies only to accepted data.")
    issues = review.get("issues") or []
    if issues:
        st.dataframe(pd.DataFrame([
            {"Severity": issue.get("severity", "warning").upper(),
             "Issue": issue.get("title", ""),
             "Finding / effect": issue.get("detail", "")}
            for issue in issues]), hide_index=True, use_container_width=True)
    else:
        st.info("No unresolved findings in the Spark quality report.")
    st.markdown("**Before you proceed**")
    st.write(review.get("approval_statement") or
             "I understand data-quality warnings could compromise model accuracy.")
    st.caption("Until you approve, no weather fetch, station registry entry, Kafka topic or model training is started.")
    acknowledged = st.checkbox(
        "I understand the data-quality risks and want to continue with registration and model deployment.",
        value=False, key=f"quality_ack_{job['id']}",
    )
    yes, no = st.columns(2)
    with yes:
        if st.button("Confirm and continue", disabled=not acknowledged, type="primary",
                     use_container_width=True, key=f"quality_yes_{job['id']}"):
            try:
                api_post(f"/stations/onboard/jobs/{job['id']}/quality-decision",
                         json={"approved": True}, timeout=15)
                st.session_state.pop("quality_dialog_job_id", None)
                st.success("Approval recorded. Backend processing will resume.")
                rerun_page()
            except requests.RequestException as exc:
                st.error(f"Could not confirm approval: {exc}")
    with no:
        if st.button("Cancel registration", use_container_width=True,
                     key=f"quality_no_{job['id']}"):
            try:
                api_post(f"/stations/onboard/jobs/{job['id']}/quality-decision",
                         json={"approved": False}, timeout=15)
                st.session_state.pop("quality_dialog_job_id", None)
                st.info("Cancellation recorded. No station will be registered.")
                rerun_page()
            except requests.RequestException as exc:
                st.error(f"Could not cancel registration: {exc}")


if hasattr(st, "dialog"):
    @st.dialog("Data quality review — approve before model training")
    def _quality_review_dialog(job_id: str):
        try:
            job = api_get(f"/stations/onboard/jobs/{job_id}")
            _render_quality_review(job)
        except requests.RequestException as exc:
            st.error(f"Could not retrieve the data-quality review: {exc}")


# The backend preserves audit history, but the customer-facing panel shows
# only the newest registration. Terminal results are shown in the browser
# session that submitted or followed that job, never from older sessions.
st.subheader("Registration and model deployment progress")


def _latest_visible_registration(jobs: list[dict], watched_id: str | None) -> dict | None:
    if not jobs:
        return None
    latest = jobs[0]  # API returns newest first; no historical job selector.
    running = {"queued", "processing", "awaiting_confirmation", "cancelling", "training"}
    if latest.get("status") in running or latest.get("id") == watched_id:
        return latest
    return None


def _render_onboarding_jobs():
    try:
        jobs = api_get("/stations/onboard/jobs?limit=1").get("jobs", [])
    except requests.RequestException as exc:
        st.warning(f"Cannot retrieve registration progress: {exc}")
        return
    latest = _latest_visible_registration(jobs, st.session_state.get("onboarding_job_id"))
    if latest is None:
        st.session_state.pop("quality_dialog_job_id", None)
        st.session_state.pop("_latest_registration_id", None)
        st.caption("No registration is running. Submit a raw file to view its progress here.")
        return
    selected_job_id = latest["id"]
    st.session_state["_latest_registration_id"] = selected_job_id
    # A refreshed browser can follow a still-running job through to completion.
    if latest.get("status") in {"queued", "processing", "awaiting_confirmation", "cancelling", "training"}:
        st.session_state["onboarding_job_id"] = selected_job_id
    # Never open an older job's quality dialog after a new upload.
    if st.session_state.get("quality_dialog_job_id") not in (None, selected_job_id):
        st.session_state.pop("quality_dialog_job_id", None)
    st.caption(f"Latest registration: {latest.get('station', 'Station')} · "
               f"{latest.get('status', 'unknown').replace('_', ' ').title()}")
    try:
        job = api_get(f"/stations/onboard/jobs/{selected_job_id}")
    except requests.RequestException as exc:
        st.warning(f"Could not refresh job progress: {exc}")
        return
    state = job.get("status", "unknown")
    st.markdown(f"**{job['station']}** · {job.get('stage', state).replace('_', ' ').title()}")
    st.caption(job.get("message", ""))
    if state == "awaiting_confirmation":
        st.warning("⚠️ Data quality needs your approval. Weather downloads, registration, Kafka setup "
                   "and training have NOT started. Review the report to proceed or cancel.")
        if st.button("Review quality findings and decide", key=f"open_quality_{selected_job_id}"):
            st.session_state["quality_dialog_job_id"] = selected_job_id
            rerun_page()
        prompted = st.session_state.setdefault("quality_prompted_job_ids", [])
        if selected_job_id not in prompted:
            prompted.append(selected_job_id)
            st.session_state["quality_dialog_job_id"] = selected_job_id
            rerun_page()
    elif state in {"queued", "processing", "training", "cancelling"}:
        if state == "training" and job.get("models_total"):
            ready, total = job.get("models_ready", 0), job["models_total"]
            st.progress(ready / total, text=f"Model deployment: {ready} of {total} complete")
        else:
            st.info("Backend processing is running. You can switch pages without stopping this job.")
    elif state == "completed":
        st.success(job.get("message") or "All selected models deployed. Ready to activate.")
    elif state in {"failed", "interrupted"}:
        st.error(job.get("message") or job.get("error") or "Registration interrupted")
    elif state == "cancelled":
        st.info("Registration cancelled at customer request. No station or model was deployed.")
    models = job.get("models") or {}
    if models:
        data = [{"Model": name.replace("_", " ").title(),
                 "Algorithm": item.get("algorithm", "—"),
                 "Status": item.get("status", "—").title(),
                 "Error": item.get("error", "")}
                for name, item in models.items()]
        st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)
    # Show completed and pending stages, not only the currently running one.
    stages = [
        ("accepted", "Receive raw file"),
        ("validation", "Validate inputs"),
        ("spark", "Spark processing"),
        ("quality", "Validate cleaned data"),
        ("quality_review", "Customer quality approval"),
        ("weather", "Download & align weather"),
        ("preparation", "Prepare training/replay datasets"),
        ("registry", "Save station configuration"),
        ("topics", "Create Kafka topics"),
        ("training", "Train and deploy selected models"),
    ]
    seen = {e.get("stage") for e in (job.get("events") or [])}
    active_stage = job.get("stage", "accepted")
    stage_order = [s for s, _ in stages]
    last_step = next((e.get("stage") for e in reversed(job.get("events") or [])
                      if e.get("stage") in stage_order), "accepted")
    active_index = stage_order.index(active_stage) if active_stage in stage_order else -1
    process_rows = []
    for index, (key, name) in enumerate(stages):
        if key == "quality_review" and state == "awaiting_confirmation":
            state_text = "Awaiting customer decision"
        elif key not in seen and key not in {"training"}:
            state_text = "Not started"  # Some optional stages may be skipped.
        elif state == "completed":
            state_text = "Completed" if key in seen else "Not applicable"
        elif state in {"failed", "interrupted"} and key == last_step:
            state_text = "Failed" if state == "failed" else "Interrupted"
        elif key == active_stage or (state == "training" and key == "training"):
            state_text = "In progress"
        elif key in seen and (active_index > index or state in {"training", "failed", "interrupted", "cancelled"}):
            state_text = "Completed"
        else:
            state_text = "Pending"
        if key == "topics" and job.get("result") and not job["result"].get("topics_ready", False):
            state_text = "Pending Kafka connection"
        process_rows.append({"Process": name, "Progress": state_text})
    st.dataframe(pd.DataFrame(process_rows), use_container_width=True, hide_index=True)
    if active_stage == "spark" or (state in {"failed", "interrupted"} and "spark" in seen):
        with st.expander("Spark processing log (latest 30 lines)", expanded=False):
            try:
                raw_log = api_get(f"/stations/onboard/jobs/{selected_job_id}/spark-log")
                if raw_log.get("available"):
                    st.code("\n".join(raw_log.get("lines", [])) or "Spark started; waiting for its first log line")
                else:
                    st.caption("The Spark process has not written a log yet")
            except requests.RequestException as exc:
                st.caption(f"Spark log unavailable: {exc}")
    with st.expander("Detailed event timeline", expanded=True):
        events = job.get("events") or []
        if events:
            st.dataframe(pd.DataFrame(events), use_container_width=True, hide_index=True)
        else:
            st.caption("No recorded events")
    st.caption(f"Last backend update: {job.get('updated', '—')} · Job ID: {job['id'][:12]}")
    if state in {"completed", "failed", "interrupted", "cancelled"}:
        if st.button("Dismiss latest result", key=f"dismiss_onboarding_{selected_job_id}"):
            st.session_state.pop("onboarding_job_id", None)
            st.session_state.pop("quality_dialog_job_id", None)
            st.session_state.pop("_latest_registration_id", None)
            rerun_page()


if hasattr(st, "fragment"):
    st.fragment(run_every="2s")(_render_onboarding_jobs)()
else:
    _render_onboarding_jobs()

# Called in the main script, outside the polling fragment. st.dialog behaves
# like a fragment, so nesting it inside the polling fragment is unsafe.
_quality_popup_id = st.session_state.get("quality_dialog_job_id")
if _quality_popup_id != st.session_state.get("_latest_registration_id"):
    st.session_state.pop("quality_dialog_job_id", None)
    _quality_popup_id = None
if _quality_popup_id:
    if hasattr(st, "dialog"):
        _quality_review_dialog(_quality_popup_id)
    else:
        st.warning("Review the data-quality findings before continuing.")
        try:
            _render_quality_review(api_get(f"/stations/onboard/jobs/{_quality_popup_id}"))
        except requests.RequestException as exc:
            st.error(f"Could not retrieve quality review: {exc}")

st.markdown("---")

# ---------------------------------------------------------------------
# Existing station operations
# ---------------------------------------------------------------------
st.subheader("Registered stations")
try:
    registry = api_get("/stations")
    stations = registry.get("stations", {})
except requests.RequestException as exc:
    st.error(f"Could not refresh station registry: {exc}")
    st.stop()

if not stations:
    st.info("No stations are registered yet.")
    st.stop()

station_ids = list(stations.keys())
default_station = st.session_state.pop("registered_station_selection", None) or registry.get("active_station")
default_index = station_ids.index(default_station) if default_station in station_ids else 0
selected = st.selectbox(
    "Select station",
    station_ids,
    index=default_index,
    format_func=lambda key: station_label(key, stations[key]),
)
entry = stations[selected]
if entry.get("replay_start_date"):
    st.caption(f"Configured replay start · {entry['replay_start_date']} at 00:00 UTC")
if entry.get("weather"):
    locations = ", ".join(point["location"] for point in entry["weather"].get("sites", []))
    st.caption(f"Open-Meteo weather · {locations}")
selected_components = entry.get("model_components", {})
try:
    component_summary = api_get(f"/models/components?station={selected}")
    inferred_enabled = component_summary.get("enabled") or {}
    sources = component_summary.get("sources") or {}
    selected_count = sum(bool(value) for value in inferred_enabled.values())
    ready_count = sum(bool(source.get("deployment_exists")) for source in sources.values())
    render_status_strip([
        ("Model components", f"{selected_count} selected", "neutral"),
        ("Deployed", f"{ready_count} artifacts", "neutral"),
    ])
    with st.expander("Registered model components", expanded=False):
        if component_summary.get("legacy_station"):
            st.caption("Legacy station: components detected from existing datasets and checkpoints.")
        component_rows = []
        for model_id, label in model_labels.items():
            selected_for_station = bool(inferred_enabled.get(model_id, False))
            source = sources.get(model_id) or {}
            has_data = bool(source.get("registered_file_exists"))
            has_model = bool(source.get("deployment_exists"))
            if not selected_for_station:
                state = "Not selected"
            elif not has_data:
                state = "Needs training data"
            elif not has_model:
                state = "Needs model training"
            else:
                state = "Ready"
            component_rows.append({
                "Component": label,
                "Selected": "Yes" if selected_for_station else "No",
                "Training data": "Available" if has_data else "Missing",
                "Deployed model": "Available" if has_model else "Not deployed",
                "Next action": state,
            })
        st.dataframe(component_rows, hide_index=True, use_container_width=True)
        st.caption("Selected ≠ trained ≠ deployed. A station may retain a trained model while its streaming pipeline is stopped.")
        st.page_link("pages/model_management.py", label="Train or deploy a component", icon="🧠", use_container_width=False)
except requests.RequestException as exc:
    st.warning(f"Component status is unavailable: {exc}")

try:
    status = api_get(f"/stations/{selected}/status")
    model = api_get(f"/models/status?station={selected}")
except requests.RequestException as exc:
    st.error(f"Could not load station status: {exc}")
    st.stop()

@st.fragment(run_every=2.0)
def render_live_station_status(station_id: str, station_entry: dict, model_info: dict) -> None:
    """Poll status only; never refresh registration or deletion forms."""
    try:
        live_status = api_get(f"/stations/{station_id}/status")
    except requests.RequestException as exc:
        st.warning(f"Live station status temporarily unavailable: {exc}")
        return
    rows_registered = station_entry.get("registered_rows")
    demand_ready = bool(live_status.get("model_ready"))
    pending = live_status.get("initial_training_pending", False)
    tasks = live_status.get("initial_training") or {}
    render_status_strip([
        ("History", f"{rows_registered:,} rows" if isinstance(rows_registered, int) else "—", "neutral"),
        ("Demand", "Ready" if demand_ready else "Preparing model" if pending else "Needs training",
         "good" if demand_ready else "warn"),
        ("Kafka", "Connected" if live_status.get("kafka_tcp_available") else "Offline",
         "good" if live_status.get("kafka_tcp_available") else "bad"),
        ("Station", str(live_status.get("lifecycle_status", station_entry.get("status", "registered"))).replace("_", " ").title(), "neutral"),
    ])
    if tasks:
        st.dataframe([
            {"Model": model_labels.get(component, component),
             "Status": task["status"].title(),
             "Details": task.get("error", "")}
            for component, task in tasks.items()
        ], hide_index=True, use_container_width=True)
        if any(task["status"] == "failed" for task in tasks.values()):
            if st.button("Retry failed training", key=f"retry_initial_{station_id}",
                         disabled=live_status.get("lifecycle_status") in {"active", "recovering", "failed"}):
                try:
                    api_post(f"/stations/{station_id}/retry-initial-training")
                    st.rerun()
                except requests.RequestException as exc:
                    st.error(f"Could not retry training: {exc}")
        if not pending and demand_ready and live_status.get("lifecycle_status") not in {"active", "recovering"}:
            if st.button("Activate station", key=f"activate_registered_{station_id}", type="primary",
                         disabled=not live_status.get("kafka_tcp_available")):
                try:
                    api_post(f"/stations/{station_id}/activate", timeout=120)
                    st.rerun()
                except requests.RequestException as exc:
                    st.error(f"Could not activate the station: {exc}")
    with st.expander("Technical station details", expanded=False):
        st.json({k: v for k, v in station_entry.items() if k not in {"registered_source_file"}})
        services = live_status.get("services", {})
        render_status_strip([
            (name.title(), "Running" if services.get(name, {}).get("running") else "Stopped",
             "good" if services.get(name, {}).get("running") else "neutral")
            for name in ("training", "spark", "predictor", "producer")
        ])
        if model_info.get("checkpoint_exists"):
            metadata = model_info.get("metadata") or {}
            st.write(f"Model version: {model_info.get('model_version') or 'v1'}")
            rmse = metadata.get("best_validation_rmse_scaled")
            st.write(f"Validation RMSE (scaled): {rmse:.4f}" if isinstance(rmse, (int, float)) else "Validation RMSE: —")
            st.write(f"Checkpoint: `{model_info.get('checkpoint_path')}`")


render_live_station_status(selected, entry, model)

# ---------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------
with st.expander("Advanced: service logs", expanded=False):
    st.markdown("#### Station process logs")
    log_service = st.selectbox("Log source", ["training", "spark", "predictor", "producer", "live_actuals"], key="station_log_service")
    if st.button("Refresh selected log"):
        try:
            log_data = api_get(f"/pipeline/logs/{log_service}?station={selected}")
            st.code(log_data.get("content", "No log output yet."), language="text")
        except requests.RequestException as exc:
            st.error(f"Could not load log: {exc}")

if st.button("Refresh station status"):
    rerun_page()



# ---------------------------------------------------------------------
# Permanent station deletion
# ---------------------------------------------------------------------
st.markdown("---")
st.subheader("Danger zone — Delete station")
st.warning(
    "This permanently removes the selected station from the registry, deletes its "
    "configured input/history/training CSVs, model checkpoint and metadata, station "
    "Spark checkpoint, station-specific logs, and its rows in actuals.csv and forecasts.csv. "
    "Its Kafka records are cleared, but the empty Kafka topic definitions are retained "
    "to avoid Windows KRaft topic-directory deletion errors. Other registered stations "
    "and their data are preserved."
)
with st.expander("Delete this station and all its data", expanded=False):
    st.write(f"Selected station: **{selected}** — {entry.get('display_name', selected)}")
    st.caption(
        "The operation is blocked while this station has running producer, Spark, predictor "
        "or training processes. It is also blocked if a topic or configured file is shared "
        "with another station, or if another station's predictor is writing to the shared result CSVs."
    )
    delete_ack = st.checkbox(
        "I understand this permanently deletes this station's local data, model and saved results.",
        key=f"delete_ack_{selected}",
    )
    typed_station = st.text_input(
        f"Type {selected} to confirm",
        key=f"delete_station_text_{selected}",
        placeholder=selected,
    )
    services_running = any(
        status.get("services", {}).get(name, {}).get("running", False)
        for name in ("training", "spark", "predictor", "producer")
    )
    if services_running:
        st.info("Stop all services for this station before deleting it.")
    if st.button(
        f"Delete {selected} permanently",
        type="secondary",
        use_container_width=True,
        disabled=(not delete_ack or typed_station.strip().lower() != selected.lower() or services_running),
        key=f"delete_station_button_{selected}",
    ):
        try:
            with st.spinner(f"Deleting {selected} and clearing its records…"):
                deletion = api_delete(
                    f"/stations/{selected}",
                    json={"confirm_station_id": typed_station},
                    timeout=300,
                )
            st.success(deletion.get("message", f"Station {selected} deleted."))
            st.json(deletion)
            rerun_page()
        except requests.HTTPError as exc:
            detail = exc.response.text if exc.response is not None else str(exc)
            st.error(f"Station deletion failed: {detail}")
        except requests.RequestException as exc:
            st.error(f"Station deletion failed: {exc}")
