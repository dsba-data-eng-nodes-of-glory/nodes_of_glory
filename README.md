# Nodes of Glory — Latest Registration Only Hotfix

**Baseline:** Apply after `Nodes_of_Glory_Data_Quality_Approval_Patch.zip` and the earlier location/background-registration patches. Does not touch training, Spark, Kafka, station configs, datasets, models or checkpoints.

## Changes

- Station Management no longer shows a dropdown of historical completed/interrupted registration jobs. It requests **one newest job** and shows only its progress.
- On page navigation, an active latest job and pending quality confirmation remain visible. Its completion or failure stays visible in the current browser session until the operator clicks **Dismiss latest result**.
- On a new browser session, **stale completed, failed, cancelled, or interrupted jobs do not reappear** in the progress panel. They remain stored in the backend database for diagnosis, without being deleted.
- The registration reminder on other dashboard pages also checks only the newest job; older pending/failed jobs no longer appear in the customer-facing banner.
- If two jobs have the same creation second, the API picks the most recently inserted one.

**Important:** This is a *display-only* change. Older registration jobs are not cancelled; any backend jobs already running continue, and their audit records remain. Only the newest is visible in the customer UI. Do not submit multiple parallel jobs if you need to approve each separately.

## Safe installation (Windows)

1. Stop Streamlit and FastAPI; back up your project folder.
2. Extract this ZIP anywhere.
3. In a command prompt, run:

```bat
python preflight_and_apply.py --project "C:\Users\hp\Documents\Nodes of Glory\Performance upgrade" --check
```

4. If it prints `Preflight PASSED`, run:

```bat
python preflight_and_apply.py --project "C:\Users\hp\Documents\Nodes of Glory\Performance upgrade" --apply
```

5. Restart FastAPI and Streamlit.

The installer refuses to overwrite files that differ from the **Data Quality Approval** patch baseline. If you have made local changes, inspect the included diff and merge manually rather than forcing a replacement.

## Verification

- `python -m py_compile overlay/dashboard/pages/station_management.py overlay/dashboard/ui.py overlay/backend/onboarding_jobs.py`
- `python -m pytest -q tests/test_latest_only.py` — 6 focused tests (no Spark or Streamlit installation needed).

Not tested with the user's running Windows Streamlit/FastAPI environment.
