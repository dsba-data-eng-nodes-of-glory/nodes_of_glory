"""Focused compatibility tests that require neither Streamlit nor a Spark runtime."""
from __future__ import annotations
import ast
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / 'overlay'
PAGE = ROOT / 'dashboard/pages/station_management.py'
UI = ROOT / 'dashboard/ui.py'
JOBS = ROOT / 'backend/onboarding_jobs.py'


def _selector():
    module = ast.parse(PAGE.read_text(encoding='utf8'))
    node = next(n for n in module.body if isinstance(n, ast.FunctionDef)
                and n.name == '_latest_visible_registration')
    scope = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(PAGE), 'exec'),
         {'__builtins__': __builtins__, 'list': list, 'dict': dict}, scope)
    return scope['_latest_visible_registration']


def test_latest_running_job_only():
    select = _selector()
    latest = {'id': 'new', 'station': 'tennet', 'status': 'processing'}
    older = {'id': 'old', 'station': '50_hertz', 'status': 'interrupted'}
    assert select([latest, older], 'old') == latest
    assert select([latest, older], None) == latest


def test_older_session_terminal_is_hidden():
    select = _selector()
    for state in ['interrupted', 'completed', 'failed', 'cancelled']:
        latest = {'id': 'new', 'status': state}
        assert select([latest], None) is None
        assert select([latest], 'old') is None
        assert select([latest], 'new') == latest


def test_latest_pending_quality_always_visible():
    select = _selector()
    assert select([{'id': 'a', 'status': 'awaiting_confirmation'}], None)['id'] == 'a'


def test_page_has_no_historical_job_selector():
    src = PAGE.read_text(encoding='utf8')
    assert '"/stations/onboard/jobs?limit=1"' in src
    assert 'st.selectbox(\n        "Registration job"' not in src
    assert 'Dismiss latest result' in src
    assert 'quality_dialog_job_id' in src


def test_global_navigation_badge_uses_only_latest():
    src = UI.read_text(encoding='utf8')
    assert 'params={"limit": 1}' in src
    assert 'pending_confirmation = [j for j in jobs' not in src


def test_backend_ties_resolve_to_last_inserted():
    src = JOBS.read_text(encoding='utf8')
    assert 'ORDER BY created DESC, rowid DESC LIMIT ?' in src
    with sqlite3.connect(':memory:') as db:
        db.execute('CREATE TABLE jobs(id TEXT, created TEXT)')
        db.executemany('INSERT INTO jobs VALUES(?,?)',
                       [('older', '2026-11-21T10:30:00'),
                        ('newer', '2026-11-21T10:30:00')])
        assert db.execute('SELECT id FROM jobs ORDER BY created DESC, rowid DESC LIMIT 1').fetchone()[0] == 'newer'
