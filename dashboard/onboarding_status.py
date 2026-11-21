"""Run station data submission separately from the Streamlit progress panel."""
from concurrent.futures import Future
from threading import Thread
from uuid import uuid4

import requests
import streamlit as st


STAGES = {
    "upload": 0,
    "validation": 1,
    "weather": 2,
    "model_data": 3,
    "registration": 4,
    "topics": 5,
    "training": 6,
    "training_failed": 6,
    "complete": 7,
}


def submit_registration(api, data, upload_file):
    progress_id = str(uuid4())
    result = Future()

    def upload():
        try:
            response = requests.post(
                f"{api}/stations/onboard",
                data={**data, "progress_id": progress_id},
                files={"file": upload_file},
                timeout=(10, 3600 if data.get("upload_format") == "raw" else 900),
            )
            result.set_result(response)
        except requests.RequestException as exc:
            result.set_exception(exc)

    Thread(target=upload, daemon=True, name="station-registration").start()
    return {"id": progress_id, "station": data["station_id"], "result": result}


def _error(response):
    try:
        detail = response.json().get("detail")
        if detail:
            return str(detail)
    except (ValueError, AttributeError):
        pass
    return f"Registration returned HTTP {response.status_code}."


def _refresh(api, job):
    try:
        response = requests.get(f"{api}/stations/onboard/progress/{job['id']}", timeout=(3, 4))
        response.raise_for_status()
        job["snapshot"] = response.json()
    except (requests.RequestException, ValueError):
        status_unavailable = True
    else:
        status_unavailable = False
    snapshot = job.get("snapshot") or {}
    future = job["result"]
    if future.done() and snapshot.get("stage") not in {"weather_limited", "failed"}:
        try:
            response = future.result()
        except requests.RequestException:
            if status_unavailable:
                return {"stage": "pending", "message": "Connection interrupted. Check Registered stations before resubmitting."}
        else:
            if not response.ok:
                return {"stage": "failed", "message": _error(response)}
            if not snapshot or STAGES.get(snapshot["stage"], 0) < STAGES["training"]:
                registration = response.json()
                return {
                    "stage": "training", "station": registration["station"], "message": "Station saved. Waiting for training status.",
                    "tasks": registration.get("initial_training", {}),
                }
    return snapshot or {"stage": "upload", "message": "Sending the station file to the registration service."}


@st.fragment(run_every=2.0)
def render_registration_progress(api, job, labels):
    snapshot = _refresh(api, job)
    stage = snapshot["stage"]
    message = snapshot["message"]
    st.subheader(f"Registration progress · {snapshot.get('station', job['station'])}")
    if stage in {"weather_limited", "failed"}:
        st.error(message)
        return
    st.caption("Upload → validation → weather → model data → registration → Kafka topics → training and deployment")
    quality = snapshot.get("spark_import")
    if quality:
        kept = quality["rows"] - quality["edge_rows_trimmed"]
        imputed = sum(quality["cells_imputed"].values())
        st.caption(f"Spark prepared {kept:,} intervals · {quality['missing_timestamp_pct']:.2f}% source timestamp gaps "
                   f"· {imputed:,} values filled from history.")
        if quality["edge_rows_trimmed"]:
            st.caption(f"Skipped {quality['edge_rows_trimmed']:,} leading intervals before a complete historical baseline.")
    tasks = snapshot.get("tasks") or {}
    ready = sum(task["status"] == "ready" for task in tasks.values())
    progress = STAGES.get(stage, 0)
    if stage in {"training", "training_failed"} and tasks:
        progress += ready / len(tasks)
    st.progress(progress / 7)
    if stage == "complete":
        st.success(message)
        if snapshot.get("topics_ready"):
            st.caption("Open Registered stations to activate the station.")
        elif "demand_forecast" in tasks:
            st.warning(snapshot.get("topic_warning") or "Kafka topics are pending. Check Kafka before activating.")
    elif stage == "pending":
        st.warning(message)
    elif stage == "training_failed":
        st.error(message)
        st.caption("Open Registered stations to retry failed models.")
    else:
        st.info(message)
    if tasks:
        st.caption(f"{ready} of {len(tasks)} models deployed")
        st.dataframe([
            {"Model": labels.get(name, name),
             "Status": {"ready": "Deployed", "running": "Training or deploying"}.get(task["status"], task["status"].title()),
             "Details": task.get("error", "")}
            for name, task in tasks.items()
        ], hide_index=True, use_container_width=True)
    if job["result"].done() and stage in {"training", "training_failed", "complete"}:
        if st.button("Refresh registered stations", key=f"refresh_registration_{job['id']}"):
            st.session_state["registered_station_selection"] = snapshot["station"]
            st.rerun()
