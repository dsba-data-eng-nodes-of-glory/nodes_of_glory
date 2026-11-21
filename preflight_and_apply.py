"""Safely install the Nodes of Glory latest-onboarding-job UI patch.

Usage (run from extracted ZIP folder):
  python preflight_and_apply.py --project "C:\\...\\Performance upgrade" --check
  python preflight_and_apply.py --project "C:\\...\\Performance upgrade" --apply
"""
from __future__ import annotations
import argparse
import hashlib
import shutil
from datetime import datetime, timezone
from pathlib import Path

FILES = [
    "dashboard/pages/station_management.py",
    "dashboard/ui.py",
    "backend/onboarding_jobs.py",
]
EXPECTED = {
    # Baseline: Nodes_of_Glory_Data_Quality_Approval_Patch.zip, overlay files.
    "dashboard/pages/station_management.py": "e81eb62d7e2f957d00870f04d048d5ff7dd385c97f291b1b4666b0e2a6908033",  # populated by build script
    "dashboard/ui.py": "a5718c58d7c74595f5a3bd781a8de95b56c6b46940eaa7b5f2680d66a901e9a2",
    "backend/onboarding_jobs.py": "670ed1810b12b7c35b130f10fc1555dc65aeb5de58138547d1c2d697d2b4042c",
}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True, help='Full project root path')
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--check', action='store_true')
    action.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    project = Path(args.project).expanduser().resolve()
    overlay = Path(__file__).resolve().parent / 'overlay'
    checks = []
    for rel in FILES:
        dest = project / rel
        src = overlay / rel
        if not dest.is_file():
            raise SystemExit(f'ERROR: Missing original file: {dest}')
        if not src.is_file():
            raise SystemExit(f'ERROR: Missing replacement file: {src}')
        old_hash = digest(dest)
        new_hash = digest(src)
        if old_hash == new_hash:
            state = 'already patched'
        elif old_hash == EXPECTED[rel]:
            state = 'compatible'
        else:
            raise SystemExit(
                f'ERROR: Local file differs from the tested Data Quality Approval baseline: {dest}\n'
                'No files have been changed. Compare the included LATEST_ONLY_CHANGES.patch '
                'with your local edits before applying.'
            )
        checks.append((rel, state))
    for rel, state in checks:
        print(f'{rel}: {state}')
    if args.check:
        print('Preflight PASSED; no files changed.')
        return
    backup = project / ('backup_before_latest_job_patch_' +
                        datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f'))
    for rel, state in checks:
        if state == 'already patched':
            continue
        src = project / rel
        saved = backup / rel
        saved.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, saved)
        shutil.copy2(overlay / rel, src)
    print(f'Applied. Original files backed up in {backup}.' if backup.exists() else 'Already installed.')
    print('Restart Streamlit; restart FastAPI only if backend/onboarding_jobs.py was updated.')


if __name__ == '__main__':
    main()
