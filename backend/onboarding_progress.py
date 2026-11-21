"""Short-lived registration status for the dashboard's polling fragment."""
from collections import OrderedDict
from threading import Lock
from time import monotonic

_PROGRESS = OrderedDict()
_LOCK = Lock()
_MAX_RECORDS = 128
_TTL_SECONDS = 3600


def record_progress(progress_id, stage, message, **details):
    if progress_id is None:
        return
    with _LOCK:
        now = monotonic()
        while _PROGRESS and now - next(iter(_PROGRESS.values()))[0] >= _TTL_SECONDS:
            _PROGRESS.popitem(last=False)
        _PROGRESS[progress_id] = (now, {"stage": stage, "message": message, **details})
        _PROGRESS.move_to_end(progress_id)
        while len(_PROGRESS) > _MAX_RECORDS:
            _PROGRESS.popitem(last=False)


def read_progress(progress_id):
    with _LOCK:
        record = _PROGRESS.get(progress_id)
        now = monotonic()
        if record and now - record[0] < _TTL_SECONDS:
            if record[1]["stage"] == "training":
                _PROGRESS[progress_id] = (now, record[1])
                _PROGRESS.move_to_end(progress_id)
            return record[1].copy()
    return {}
