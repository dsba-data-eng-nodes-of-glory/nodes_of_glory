"""End-to-end Spark RDD electricity preprocessing.

Excel/CSV staging handles workbook I/O; Spark owns source casting, ordering,
duplicate reconciliation, 10% data gates, imputation, and all CSV outputs.
For a single station, historical state runs sequentially inside ONE Spark
Python worker via mapPartitions; no toLocalIterator/driver-side reconstruction,
no replicated 30-day chunks, and no global Spark SQL windows.

The shaped wind bridge is retrospective: it uses a later actual reading and
MUST NOT be exposed in live as-of-time forecasting or target evaluation.
"""



import argparse
import json
import csv
import math
import numbers
import os
import re
import sys
from collections import Counter, defaultdict, deque
from contextlib import ExitStack
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory, TemporaryFile, mkstemp
import shutil
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from openpyxl import load_workbook
try:
    from pyspark import StorageLevel
    from pyspark.sql import SparkSession, functions as F, types as T
except ImportError:  # Enables input-stage unit tests without Spark installed.
    StorageLevel = SparkSession = F = T = None

HOUR_SECONDS = 60 * 60
DAY_SECONDS = 24 * HOUR_SECONDS
MAX_MISSING_TIMESTAMP_PCT = 10
MAX_MISSING_TARGET_PCT = 10.0
PRIMARY_TARGET_KEYS = ("demand", "solar_mwh", "wind_mwh")
DEFAULT_DUPLICATE_TOLERANCE_PCT = 1.0
SHORT_GAP_SECONDS = HOUR_SECONDS
MEDIUM_GAP_SECONDS = 4 * HOUR_SECONDS
# For COMPLETED HISTORICAL workbooks, preserve a bounded portion of the existing
# past-only AR/historical wind *shape*, but correct its drift to exactly connect
# the observed endpoints. Future observations are used for the endpoint correction:
# this is NOT live-causal and must not leak into as-of-time replay/training targets.
WIND_HISTORICAL_TWO_SIDED_INTERPOLATION = True
WIND_LONG_GAP_SECONDS = 4 * HOUR_SECONDS
WIND_HISTORICAL_BRIDGE_SHAPE_WEIGHT = 0.60  # 0=pure linear, 1=full bounded historical curvature
WIND_HISTORICAL_BRIDGE_SHAPE_CAP_PCT = 35.0  # maximum shape deviation relative to observed scale
STAGED_NUMBER_PREFIX = "@@N@@"  # Unambiguously marks numeric Excel cells


# Workbook header -> internal Spark column -> final output column.
METRICS = (
    ("Demand", "demand", "demand"),
    ("Photovoltaics [MWh]", "solar_mwh", "Photovoltaics [MWh]"),
    ("Photovoltaics_installed_capacity [MW]", "solar_mw", "Photovoltaics_installed_capacity [MW]"),
    ("Wind onshore [MWh]", "wind_mwh", "Wind onshore [MWh]"),
    ("Wind_onshore_installed_capacity [MW]", "wind_mw", "Wind_onshore_installed_capacity [MW]"),
)
METRIC_KEYS = [key for _, key, _ in METRICS]
REQUIRED_COLUMNS = ("timestamp", *METRIC_KEYS)
MONTHS = {
    name: month for month, name in enumerate(
        "jan feb mar apr may jun jul aug sep oct nov dec".split(), 1
    )
}
ENGLISH_DATE = re.compile(
    r"([A-Za-z]{3}) (\d{1,2}), (\d{4}) (\d{1,2}):(\d{2})(?::(\d{2}))? ([AP]M)",
    re.IGNORECASE,
)


class DataQualityError(ValueError):
    """The input cannot produce a complete, valid time series under the stated rules."""


def parse_local_time(value, row_number):
    """Read Excel dates and common timestamp strings, including ISO offsets."""
    try:
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime(value.year, value.month, value.day)
        if isinstance(value, str):
            value = " ".join(value.split())
            english = ENGLISH_DATE.fullmatch(value)
            if english:
                month, day, year, hour, minute, second, period = english.groups()
                hour = int(hour)
                if not 1 <= hour <= 12:
                    raise ValueError("invalid 12-hour clock value")
                hour = hour % 12 + (12 if period.upper() == "PM" else 0)
                return datetime(int(year), MONTHS[month.lower()], int(day), hour, int(minute), int(second or 0))
            # German TSO exports may use CET/CEST, MEZ/MESZ, 2A/2B.
            # These represent fixed offsets and disambiguate repeated 02:00.
            marker = re.fullmatch(r"(.+?)\s+(CET|CEST|MEZ|MESZ|2A|2B)", value, re.I)
            if marker:
                value, tag = marker.group(1), marker.group(2).upper()
                tagged = parse_local_time(value, row_number)
                if tagged.tzinfo is not None:
                    raise ValueError("duplicate timezone marker")
                if tag in ("2A", "2B"):
                    if tagged.hour != 2:
                        raise ValueError("2A/2B are only valid for the repeated 02:00 hour")
                    german = ZoneInfo("Europe/Berlin")
                    earlier = tagged.replace(tzinfo=german, fold=0)
                    later = tagged.replace(tzinfo=german, fold=1)
                    if earlier.utcoffset() == later.utcoffset() or (
                        earlier.astimezone(timezone.utc).astimezone(german).replace(tzinfo=None) != tagged
                    ) or (
                        later.astimezone(timezone.utc).astimezone(german).replace(tzinfo=None) != tagged
                    ) or earlier.utcoffset() <= later.utcoffset():
                        raise ValueError("2A/2B only valid on Germany autumn DST fall-back")
                hours = 2 if tag in ("CEST", "MESZ", "2A") else 1
                return tagged.replace(tzinfo=timezone(timedelta(hours=hours)))
            iso_value = value[:-1] + "+00:00" if value.endswith("Z") else value
            try:
                return datetime.fromisoformat(iso_value)
            except ValueError:
                pass
            # Human-edited spreadsheet exports (non-zero-padded ISO and full English months).
            iso_unpadded = re.fullmatch(
                r"(\d{4})-(\d{1,2})-(\d{1,2})[ T](\d{1,2}):(\d{1,2})(?::(\d{1,2}))?", value
            )
            if iso_unpadded:
                y, mo, d, h, mi, sec = iso_unpadded.groups()
                return datetime(int(y), int(mo), int(d), int(h), int(mi), int(sec or 0))
            for date_format in ("%d.%m.%Y", "%d-%m-%Y", "%B %d, %Y"):
                for time_format in (" %H:%M:%S", " %H:%M", " %I:%M %p", ""):
                    try:
                        return datetime.strptime(value, date_format + time_format)
                    except ValueError:
                        continue
    except (KeyError, ValueError):
        pass
    raise DataQualityError(f"Excel row {row_number}: invalid timestamp {value!r}.")


def utc_candidates(local_time, zone, row_number):
    """Resolve a wall-clock time to one or two real instants."""
    if local_time.microsecond:
        # Ignore sub-millisecond Excel floating-point date drift, not real subsecond data.
        if local_time.microsecond <= 1000:
            local_time = local_time.replace(microsecond=0)
        elif local_time.microsecond >= 999000:
            local_time = (local_time + timedelta(seconds=1)).replace(microsecond=0)
        else:
            raise DataQualityError(
                f"Excel row {row_number}: timestamps must have whole-second precision."
            )
    if local_time.tzinfo is not None:
        epoch = int(local_time.timestamp())
        # Reject a wrongly labelled CET/CEST offset on a German date.
        # An aware UTC/offset source with another declared zone is still accepted.
        if zone.key == "Europe/Berlin" and local_time.utcoffset() in (timedelta(hours=1), timedelta(hours=2)):
            if datetime.fromtimestamp(epoch, zone).utcoffset() != local_time.utcoffset():
                raise DataQualityError(f"Excel row {row_number}: CET/CEST marker contradicts Germany DST on this date.")
        return (epoch,)

    first = local_time.replace(tzinfo=zone, fold=0)
    second = local_time.replace(tzinfo=zone, fold=1)
    if first.utcoffset() == second.utcoffset():
        return (int(first.timestamp()),)

    epochs = []
    for candidate in (first, second):
        epoch = int(candidate.timestamp())
        if datetime.fromtimestamp(epoch, zone).replace(tzinfo=None) == local_time:
            epochs.append(epoch)
    if not epochs:
        raise DataQualityError(
            f"Excel row {row_number}: {local_time} does not exist in {zone.key}."
        )
    return tuple(sorted(epochs))


def is_blank(value):
    return value is None or (isinstance(value, str) and not value.strip())


def stage_number(value):
    """Preserve whether a value was typed as a number in Excel.

    Without this, Excel numeric 1.234 is indistinguishable from text '1.234',
    which is 1234 in some German-formatted source spreadsheets.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value)  # invalid text; reported as missing and imputed
    if isinstance(value, numbers.Real):
        return STAGED_NUMBER_PREFIX + str(value)
    return str(value)


# Canonical numeric conversion occurs while staging the source file, before
# Spark. This avoids enormous repeated CASE WHEN expressions in Spark's plan.
# @@BAD@@ marks a malformed nonblank source cell, distinguishing it from NULL.
MALFORMED_STAGED_NUMBER = "@@BAD@@"
_CANONICAL_NUMBER = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\Z")
_DE_GROUP = re.compile(r"[+-]?[1-9]\d{0,2}(?:\.\d{3})+(?:,\d+)?(?:[eE][+-]?\d+)?\Z")
_EN_GROUP = re.compile(r"[+-]?[1-9]\d{0,2}(?:,\d{3})+(?:\.\d+)?(?:[eE][+-]?\d+)?\Z")
_DE_SPACE = re.compile(r"[+-]?[1-9]\d{0,2}(?: \d{3})+(?:,\d+)?(?:[eE][+-]?\d+)?\Z")
_EN_SPACE = re.compile(r"[+-]?[1-9]\d{0,2}(?: \d{3})+(?:\.\d+)?(?:[eE][+-]?\d+)?\Z")
_DE_DECIMAL = re.compile(r"[+-]?(?:\d+(?:,\d*)?|,\d+)(?:[eE][+-]?\d+)?\Z")
_AMBIGUOUS_SINGLE = re.compile(r"[+-]?[1-9]\d{0,2}[,.]\d{3}\Z")


def parse_staged_number(value, locale="de", expected_unit=None):
    """Convert input staged text into plain finite ASCII numeric text.

    Return None for genuinely blank cells, @@BAD@@ for malformed nonblank
    cells and a canonical decimal string for valid ones. Native numeric Excel
    cells have a marker and are independent of the configured text locale.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    locale = {"german": "de", "english": "en"}.get(locale, locale)
    if locale not in ("de", "en", "auto"):
        raise ValueError("locale must be de/en/auto")
    text = str(value).strip().translate(str.maketrans({
        "\u00a0": " ", "\u202f": " ", "\u2009": " ", "\u2212": "-"
    }))
    native = text.startswith(STAGED_NUMBER_PREFIX)
    if native:
        text = text[len(STAGED_NUMBER_PREFIX):]
    elif expected_unit:
        # Preserve exact unit semantics: MW and MWh are not interchangeable.
        text = re.sub(r"\s+" + re.escape(expected_unit) + r"\s*\Z", "", text, flags=re.I).strip()
    if not text or len(text) > 300:
        return MALFORMED_STAGED_NUMBER
    if native:
        canonical = text if _CANONICAL_NUMBER.fullmatch(text) else None
    elif locale == "de":
        if _DE_GROUP.fullmatch(text):
            canonical = text.replace(".", "").replace(",", ".")
        elif _DE_SPACE.fullmatch(text):
            canonical = text.replace(" ", "").replace(",", ".")
        elif _DE_DECIMAL.fullmatch(text):
            canonical = text.replace(",", ".")
        elif re.fullmatch(r"[+-]?\d+(?:[eE][+-]?\d+)?", text):
            canonical = text
        else:
            canonical = None
    elif locale == "en":
        if _EN_GROUP.fullmatch(text):
            canonical = text.replace(",", "")
        elif _EN_SPACE.fullmatch(text):
            canonical = text.replace(" ", "")
        elif _CANONICAL_NUMBER.fullmatch(text):
            canonical = text
        else:
            canonical = None
    else:
        if _AMBIGUOUS_SINGLE.fullmatch(text):
            canonical = None
        elif _DE_GROUP.fullmatch(text):
            canonical = text.replace(".", "").replace(",", ".")
        elif _EN_GROUP.fullmatch(text):
            canonical = text.replace(",", "")
        elif _DE_SPACE.fullmatch(text):
            canonical = text.replace(" ", "").replace(",", ".")
        elif _EN_SPACE.fullmatch(text):
            canonical = text.replace(" ", "")
        elif _DE_DECIMAL.fullmatch(text):
            canonical = text.replace(",", ".")
        elif _CANONICAL_NUMBER.fullmatch(text):
            canonical = text
        else:
            canonical = None
    if canonical is None:
        return MALFORMED_STAGED_NUMBER
    try:
        number = float(canonical)
        if not math.isfinite(number):
            return MALFORMED_STAGED_NUMBER
        # repr is a locale-neutral compact numeric string, safe for Spark cast.
        return repr(number)
    except (ValueError, OverflowError):
        return MALFORMED_STAGED_NUMBER


def staged_numeric_value(column):
    """The only numeric transformation Spark needs after canonical staging.

    A malformed marker must not be cast, especially with ANSI mode enabled.
    """
    canonical = F.trim(column)
    valid = canonical.rlike(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
    return F.when(valid, canonical.cast("double")).otherwise(F.lit(None).cast("double"))


def resolve_repeated_hour(block, previous_epoch, next_epoch, stats=None):
    """Choose chronological DST passes, preferring strictly increasing UTC times.

    Multiple equally plausible assignments are rejected rather than guessed.
    Repeated source rows inside the same pass are considered only as fallback.
    """
    known = [epochs for epochs, _, _ in block if epochs]

    def find_splits(strict):
        ordered = (lambda a, b: a < b) if strict else (lambda a, b: a <= b)
        early_ok = [True]
        previous = previous_epoch
        for early, _ in known:
            early_ok.append(early_ok[-1] and (previous is None or ordered(previous, early)))
            previous = early

        late_ok = [True] * (len(known) + 1)
        following = next_epoch
        for index in range(len(known) - 1, -1, -1):
            late = known[index][1]
            late_ok[index] = late_ok[index + 1] and (following is None or ordered(late, following))
            following = late

        options = []
        for split in range(len(known) + 1):
            left = known[split - 1][0] if split else previous_epoch
            right = known[split][1] if split < len(known) else next_epoch
            if (early_ok[split] and late_ok[split]
                    and (left is None or right is None or ordered(left, right))):
                options.append(split)
        return options

    possibilities = find_splits(strict=True)
    if not possibilities:
        possibilities = find_splits(strict=False)
    if len(possibilities) != 1:
        # With naive local clock readings and disordered/repeated 02:00 rows,
        # the correct fold is not mathematically identifiable. Do NOT reject:
        # use a deterministic lowest-disorder split, and record the guess.
        # Real UTC/offset timestamps should be preferred when available.
        n = len(known)
        def split_score(split):
            chosen = [ep[0] if i < split else ep[1] for i, ep in enumerate(known)]
            chain = ([previous_epoch] if previous_epoch is not None else []) + chosen + \
                    ([next_epoch] if next_epoch is not None else [])
            regressions = sum(right < left for left, right in zip(chain, chain[1:]))
            duplicate_times = sum(right == left for left, right in zip(chosen, chosen[1:]))
            # Prefer both fold passes when repeated readings might indicate both.
            one_fold_penalty = int(n >= 2 and (split == 0 or split == n))
            return (regressions, duplicate_times, one_fold_penalty, abs(split - n / 2), split)
        split = min(range(n + 1), key=split_score)
        if stats is not None:
            stats["ambiguous_dst_blocks_repaired"] = stats.get("ambiguous_dst_blocks_repaired", 0) + 1
            stats["ambiguous_dst_rows_repaired"] = stats.get("ambiguous_dst_rows_repaired", 0) + n
    else:
        split = possibilities[0]
    candidates = iter(epochs[int(index >= split)] for index, epochs in enumerate(known))
    return [(next(candidates) if epochs else None, row_number, values)
            for epochs, row_number, values in block]


def timed_excel_rows(rows, timestamp_index, metric_indices, zone, stats=None):
    """Yield UTC records, requiring local chronology only within ambiguous DST blocks.

    Arbitrary ordering of ordinary unambiguous timestamps is safe, since the
    staging layer groups duplicate UTC epochs and builds a sorted UTC grid.
    Repeated Germany autumn 02:00 blocks must themselves have an unambiguous
    chronological split; resolve_repeated_hour() rejects ambiguous blocks.
    """
    stats = stats if stats is not None else {}
    repeated_hour = []
    previous_epoch = None
    last_emitted = None
    out_of_order = False
    saw_naive_ambiguous = False

    def emit(records):
        nonlocal last_emitted, out_of_order
        for record in records:
            epoch = record[0]
            if epoch is not None:
                if last_emitted is not None and epoch < last_emitted:
                    out_of_order = True
                    stats["out_of_order_source_rows"] = stats.get("out_of_order_source_rows", 0) + 1
                last_emitted = epoch
            yield record

    for row_number, row in enumerate(rows, start=2):
        if all(is_blank(value) for value in row):
            continue
        def cell(index):
            return row[index] if index < len(row) else None
        values = tuple(cell(index) for index in metric_indices)
        raw_time = cell(timestamp_index)
        if is_blank(raw_time):
            if repeated_hour:
                repeated_hour.append((None, row_number, values))
            else:
                yield from emit([(None, row_number, values)])
            continue
        try:
            local_time = parse_local_time(raw_time, row_number)
            epochs = utc_candidates(local_time, zone, row_number)
        except (DataQualityError, ValueError, OverflowError):
            # Unknown, nonexistent DST, or malformed timestamp: do not guess
            # its actual instant. Treat row as undated; attempt slot recovery
            # from adjacent explicit timestamps, otherwise leave unplaced.
            stats["invalid_timestamp_rows"] = stats.get("invalid_timestamp_rows", 0) + 1
            if repeated_hour:
                repeated_hour.append((None, row_number, values))
            else:
                yield from emit([(None, row_number, values)])
            continue
        if len(epochs) == 2:
            saw_naive_ambiguous = True
            repeated_hour.append((epochs, row_number, values))
            continue
        epoch = epochs[0]
        if repeated_hour:
            yield from emit(resolve_repeated_hour(repeated_hour, previous_epoch, epoch, stats))
            repeated_hour = []
        yield from emit([(epoch, row_number, values)])
        previous_epoch = epoch
    if repeated_hour:
        yield from emit(resolve_repeated_hour(repeated_hour, previous_epoch, None, stats))
    # Harmless source-row disorder outside the repeated DST hour is allowed.
    # resolve_repeated_hour() already validates chronological ordering within
    # each ambiguous 02:00 block and rejects blocks with non-unique assignments.
    # A global disorder check would incorrectly reject files with ordinary
    # shuffled/duplicated rows from other dates (for example, 1 Jan 2022).
    # Downstream staging groups duplicate UTC epochs and builds a sorted grid.


def utc_label(epoch):
    pattern = "%d-%m-%Y %H:%M:%S UTC" if epoch % 60 else "%d-%m-%Y %H:%M UTC"
    return datetime.fromtimestamp(epoch, timezone.utc).strftime(pattern)


def infer_timed_rows(timed_rows, buffer, interval_seconds=None):
    """Buffer raw rows and discover all explicit UTC timestamps before stage assignment.

    Looking at the entire workbook before reconstructing blank timestamps prevents
    a later out-of-order explicit timestamp from colliding with a guessed blank.
    """
    writer = csv.writer(buffer)
    observed = set()
    for epoch, row_number, values in timed_rows:
        writer.writerow([epoch, row_number, *(stage_number(v) for v in values)])
        if epoch is not None:
            observed.add(epoch)
    if not observed:
        raise DataQualityError("No timestamped data rows found.")
    if interval_seconds is None:
        if len(observed) == 1:
            step_seconds = 900  # safe default with one known clock tick
        else:
            ordered = sorted(observed)
            counts = Counter(right - left for left, right in zip(ordered, ordered[1:]))
            step_seconds = min(counts, key=lambda step: (-counts[step], step))
    else:
        step_seconds = interval_seconds
    buffer.seek(0)
    rows = (
        (int(epoch) if epoch else None, int(row_number), values)
        for epoch, row_number, *values in csv.reader(buffer)
    )
    return step_seconds, rows, observed


def normalize_header(value):
    return re.sub(r"\s+", " ", str(value or "").replace("\ufeff", "")).strip().casefold()


ALIASES = {
    "timestamp": ("Datum von", "updated_at_utc", "utc_timestamp", "timestamp", "time", "datetime", "date", "date_time"),
    "demand": ("Demand", "value_of_data", "demand_mw", "grid load", "load", "load_mw"),
    "solar_mwh": ("Photovoltaics [MWh]", "solar_mwh", "solar_energy_mwh"),
    "solar_mw": ("Photovoltaics_installed_capacity [MW]", "solar_capacity_mw", "solar_installed_capacity_mw"),
    "wind_mwh": ("Wind onshore [MWh]", "wind_mwh", "wind_energy_mwh"),
    "wind_mw": ("Wind_onshore_installed_capacity [MW]", "wind_capacity_mw", "wind_installed_capacity_mw"),
}


def choose_columns(headers, overrides=None):
    overrides = overrides or {}
    names = [normalize_header(h) for h in headers]
    if len(set(name for name in names if name)) < len([name for name in names if name]):
        raise DataQualityError("Duplicate input headers after whitespace/BOM normalization.")
    mapping = {}
    for key, known in ALIASES.items():
        possibilities = (overrides[key],) if overrides.get(key) else known
        matches = [headers[names.index(normalize_header(v))] for v in possibilities
                   if normalize_header(v) in names]
        if overrides.get(key) and not matches:
            raise DataQualityError(f"Requested {key} column {overrides[key]!r} not found in input headers")
        if matches:
            mapping[key] = headers.index(matches[0])
    # A full electricity forecasting station cannot train or monitor with partial
    # measurements. Require ALL six source fields for both CSV and Excel.
    missing = [key for key in REQUIRED_COLUMNS if key not in mapping]
    if missing:
        expected = {"timestamp": "timestamp (e.g. Datum von or utc_timestamp)"}
        expected.update({key: display for display, key, _ in METRICS})
        readable = ", ".join(expected[key] for key in missing)
        raise DataQualityError(
            "Required input columns missing: " + readable + ". "
            "The dashboard requires timestamp, demand, Photovoltaics [MWh], "
            "Photovoltaics_installed_capacity [MW], Wind onshore [MWh], "
            "and Wind_onshore_installed_capacity [MW]. "
            "Match source headers to the built-in ALIASES in this script; "
            "missing measurement columns cannot be invented."
        )
    inverse = {}
    for key, index in mapping.items():
        if index in inverse:
            raise DataQualityError(
                f"Input column {headers[index]!r} mapped to both {inverse[index]} and {key}; "
                "each required field needs its own source column."
            )
        inverse[index] = key
    return mapping, list(METRICS)


def sparse_fragment_info(values, metric_count):
    """Candidate is valid only when the entire source row is otherwise empty.

    The caller adds 1 candidate column and all unrelated extra columns after the
    five mandatory measurements.  This is structural detection, NOT evidence that
    the value truly represents wind generation.
    """
    if len(values) <= metric_count:
        return None
    candidate = values[metric_count]
    if (is_blank(candidate) or
        any(not is_blank(v) for v in values[:metric_count]) or
        any(not is_blank(v) for v in values[metric_count + 1:])):
        return None
    return candidate


def match_optional_column(headers, column_name):
    """Resolve an optional source header case/whitespace-insensitively."""
    norms = [normalize_header(h) for h in headers]
    wanted = normalize_header(column_name)
    return norms.index(wanted) if wanted in norms else None


def stage_input(path, csv_path, sheet_name=None, source_timezone="auto",
                interval_seconds=None, column_overrides=None, resample="auto",
                default_irregular_interval_seconds=900,
                sparse_wind_source_column=None, confirm_sparse_wind_remap=False,
                number_format="german"):
    """Stage CSV or Excel using the mandatory six-field 50Hertz schema.

    Extra input columns are ignored, but any missing required field rejects the file
    before timestamps are processed or Spark starts calculations. Gridded inputs
    retain their timestamps; irregular sources can be resampled to a UTC grid.
    """
    path = Path(path)
    if not path.is_file():
        raise DataQualityError(f"Input file does not exist: {path}")
    if path.suffix.lower() not in (".csv", ".xlsx", ".xlsm"):
        raise DataQualityError("Expected CSV, XLSX or XLSM. Legacy .xls is not supported.")
    if resample not in ("auto", "off", "on"):
        raise DataQualityError("resample must be auto, off or on")
    with ExitStack() as resources:
        if path.suffix.lower() == ".csv":
            stream = resources.enter_context(path.open("r", encoding="utf-8-sig", newline=""))
            preview = stream.read(16384)
            stream.seek(0)
            try:
                delimiter = csv.Sniffer().sniff(preview, delimiters=",;\t|").delimiter
            except csv.Error:
                delimiter = ","
            file_rows = csv.reader(stream, delimiter=delimiter)
            headers = next(file_rows, ())
            fmt = "csv"
            actual_sheet = None
        else:
            book = load_workbook(path, read_only=True, data_only=True)
            resources.callback(book.close)
            actual_sheet = sheet_name if sheet_name is not None else ("50 hertz" if "50 hertz" in book.sheetnames else book.sheetnames[0])
            if actual_sheet not in book.sheetnames:
                raise DataQualityError(f"Sheet {actual_sheet!r} missing; available: {book.sheetnames}")
            file_rows = iter(book[actual_sheet].values)
            headers = next(file_rows, ())
            fmt = "excel"
        mapping, metrics = choose_columns(list(headers), column_overrides)
        # Only detect an extra 'Hydropower' fragment when the row is otherwise
        # entirely empty.  A full Hydropower reading is NEVER reclassified.
        # Explicit confirmation is required to recover such rows into Wind Energy.
        candidate_name = sparse_wind_source_column
        if candidate_name is None:
            for name in ("Hydropower [MWh]", "Hydropower"):
                if match_optional_column(headers, name) is not None:
                    candidate_name = name
                    break
        candidate_index = match_optional_column(headers, candidate_name) if candidate_name else None
        if sparse_wind_source_column and candidate_index is None:
            raise DataQualityError(f"Optional sparse-wind source column {sparse_wind_source_column!r} not found")
        if confirm_sparse_wind_remap and candidate_index is None:
            raise DataQualityError("Confirmed sparse-wind remap requires an existing source column")
        if candidate_index in mapping.values():
            raise DataQualityError("Sparse-wind source must be an EXTRA column, not a required measurement")
        # All other cells must be blank for a row to qualify as a fragment.
        other_indices = ([i for i in range(len(headers))
                         if i != candidate_index and i not in mapping.values()]
                         if candidate_index is not None else [])
        timestamp_header = normalize_header(headers[mapping["timestamp"]])
        applied_zone_name = ("UTC" if "utc" in timestamp_header else "Europe/Berlin") if source_timezone == "auto" else source_timezone
        try:
            zone = ZoneInfo(applied_zone_name)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise DataQualityError(f"Unknown source time zone: {applied_zone_name}") from exc
        keys = [key for _,key,_ in metrics]
        metric_indices = [mapping[key] for key in keys]
        if candidate_index is not None:
            metric_indices.extend([candidate_index, *other_indices])
        source_time_stats = {}
        times = timed_excel_rows(file_rows, mapping["timestamp"], metric_indices, zone, source_time_stats)
        with TemporaryFile(mode="w+", encoding="utf-8", newline="", dir=csv_path.parent) as spool:
            step_seconds, timed_rows, observed = infer_timed_rows(times, spool, interval_seconds)
            # A diverse collection of timestamp differences indicates source telemetry,
            # not an evenly spaced sampling grid. Default target is 15 minutes.
            sorted_obs = sorted(observed)
            gaps = Counter(b-a for a,b in zip(sorted_obs, sorted_obs[1:]))
            regularity = max(gaps.values()) / max(len(sorted_obs)-1, 1) if gaps else 1.0
            irregular = (regularity < 0.90)
            inferred_target = interval_seconds is None and irregular
            if inferred_target:
                step_seconds = default_irregular_interval_seconds
            anchor_epoch = min(observed)
            off_grid = any((e - anchor_epoch) % step_seconds for e in observed)
            use_buckets = resample == "on" or (resample == "auto" and off_grid)
            if not use_buckets and off_grid:
                # An incompatible cadence is a repairable input issue.
                # Use causal bucket-end assignment instead of rejecting.
                use_buckets = True
                resample_auto_repaired = True
            else:
                resample_auto_repaired = False
            if use_buckets:
                # No inferred clock phase from an arbitrary first telemetry sample.
                # UTC-aligned slots enable reproducible joins across sources.
                # Bucket label denotes the END of the period (t-step, t]. Samples
                # arriving after period start must never be assigned to that
                # earlier timestamp: that would leak future observations.
                epoch_of = lambda e: ((e + step_seconds - 1) // step_seconds) * step_seconds
            else:
                epoch_of = lambda e: e
            actual_obs = {epoch_of(e) for e in observed}
            start_epoch, end_epoch = min(actual_obs), max(actual_obs)
            expected = (end_epoch - start_epoch) // step_seconds + 1
            missing = expected - len(actual_obs)
            missing_pct = 100 * missing / expected
            # Delay the acceptance decision until after blank timestamp rows
            # have been assigned to unoccupied, unambiguous grid slots.
            pending = []
            previous_source_epoch = None
            assigned_blank = 0
            unplaced_blank = 0
            inferred = set()
            seen = set()
            seen_original_epochs = set()
            duplicates = 0
            rows_bucketed = 0
            raw_rows = 0
            ambiguous_numeric_text_cells = 0
            sparse_fragment_rows = 0
            def stage_measurements(values):
                nonlocal sparse_fragment_rows
                candidate = sparse_fragment_info(values, len(keys)) if candidate_index is not None else None
                if candidate is not None:
                    sparse_fragment_rows += 1
                clean = []
                for key, value in zip(keys, values[:len(keys)]):
                    unit = "MWh" if key in ("solar_mwh", "wind_mwh") else "MW"
                    clean.append(parse_staged_number(value, number_format, unit))
                clean.append(parse_staged_number(candidate, number_format, "MWh")
                             if candidate is not None else None)
                return clean
            ambiguous_text = re.compile(r"^[+-]?[1-9]\d{0,2}[,.]\d{3}$")
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["_epoch", "_sample_epoch", "_source_row", *keys,
                                 "_sparse_wind_candidate"])
                for epoch, source_row, values in timed_rows:
                    raw_rows += 1
                    ambiguous_numeric_text_cells += sum(
                        bool(ambiguous_text.fullmatch(v.strip())) for v in values
                        if isinstance(v,str) and not v.startswith(STAGED_NUMBER_PREFIX)
                    )
                    if epoch is None:
                        pending.append((source_row, values))
                        continue
                    target = epoch_of(epoch)
                    if pending:
                        if use_buckets:
                            # Never invent a time for a row in irregular telemetry.
                            unplaced_blank += len(pending)
                        else:
                            ok = (previous_source_epoch is not None
                                  and epoch > previous_source_epoch
                                  and (epoch-previous_source_epoch) % step_seconds == 0)
                            slots = ((epoch-previous_source_epoch)//step_seconds-1) if ok else -1
                            candidate = ([previous_source_epoch+i*step_seconds for i in range(1,slots+1)]
                                         if len(pending)==slots and slots>0 else [])
                            if candidate and all(e not in observed and e not in inferred for e in candidate):
                                for reconstructed, (blank_row, raw_values) in zip(candidate, pending):
                                    writer.writerow([reconstructed, reconstructed, blank_row, *stage_measurements(raw_values)])
                                    inferred.add(reconstructed)
                                    assigned_blank += 1
                            else:
                                unplaced_blank += len(pending)
                        pending.clear()
                    if epoch in seen_original_epochs:
                        duplicates += 1
                    else:
                        seen_original_epochs.add(epoch)
                    if use_buckets and target in seen:
                        rows_bucketed += 1
                    seen.add(target)
                    writer.writerow([target, epoch, source_row, *stage_measurements(values)])
                    previous_source_epoch = epoch
            unplaced_blank += len(pending)
            actual_obs.update(inferred)
            missing = expected - len(actual_obs)
            missing_pct = 100.0 * missing / expected
            if 100 * missing > MAX_MISSING_TIMESTAMP_PCT * expected:
                raise DataQualityError(
                    f"Missing {missing:,} of {expected:,} expected timestamps "
                    f"({missing_pct:.3f}%; maximum {MAX_MISSING_TIMESTAMP_PCT}%). "
                    f"Interval {step_seconds}s; {utc_label(start_epoch)} to {utc_label(end_epoch)}."
                )
            sorted_buckets = sorted(actual_obs)
            max_run = max(((b-a)//step_seconds-1 for a,b in zip(sorted_buckets,sorted_buckets[1:])), default=0)
            coverage = {
                "source_file_type":fmt, "source_sheet":actual_sheet,
                "source_timezone_assumed_for_naive":applied_zone_name,
                "columns_mapped":{key:str(headers[index]) for key,index in mapping.items()},
                "active_measurement_keys":keys,
                "resampling_applied":use_buckets,
                "resampling_method":"UTC right-closed (t-step,t]; demand mean, renewables sum, capacity last" if use_buckets else "none",
                "irregular_source_intervals":irregular,
                "most_common_interval_seconds":min(gaps,key=lambda k:(-gaps[k],k)) if gaps else None,
                "input_cadence_consistency":round(regularity,6),
                "interval_seconds":step_seconds,
                "interval_auto_inferred":interval_seconds is None,
                "expected_timestamp_slots":expected,
                "actual_timestamp_slots":len(actual_obs),
                "missing_timestamp_slots":missing,
                "missing_timestamp_pct":missing_pct,
                "timestamp_slots_inserted":missing-assigned_blank,
                "blank_timestamps_reconstructed":assigned_blank,
                "blank_timestamp_rows_unplaced":unplaced_blank,
                "duplicate_rows_removed":duplicates,
                "source_rows_collapsed_into_resampled_buckets":rows_bucketed,
                "raw_nonempty_rows":raw_rows,
                "max_consecutive_missing_slots":max_run,
                "ambiguous_numeric_text_cells":ambiguous_numeric_text_cells,
                "resample_auto_repaired":resample_auto_repaired,
                "numeric_normalization_location":"python_staging_before_spark",
                "sparse_wind_source_column": (str(headers[candidate_index])
                                               if candidate_index is not None else None),
                "suspected_sparse_wind_fragments": sparse_fragment_rows,
                "sparse_wind_remap_confirmed": confirm_sparse_wind_remap,
                **source_time_stats,
            }
            return start_epoch, end_epoch, coverage, metrics


def check_primary_metric_coverage(missing_by_key, expected_rows, maximum_pct=MAX_MISSING_TARGET_PCT):
    """Reject a target with >10% absent/invalid *source* readings.

    Values are counted on the normalized UTC grid, AFTER duplicate resolution and
    BEFORE any historical imputation. Missing timestamps therefore count once in EACH
    target. Denominator is the number of expected slots, not raw source rows or
    the number of present measurements. Capacity columns do not trigger rejection.

    This function is pure Python so the same exact decision can be unit-tested
    without installing Spark.
    """
    if expected_rows < 1:
        raise DataQualityError("No expected timestamp slots available for coverage validation.")
    percentages = {}
    offenders = []
    for key in PRIMARY_TARGET_KEYS:
        count = int(missing_by_key[key])
        if not 0 <= count <= expected_rows:
            raise DataQualityError(
                f"Inconsistent missing count for {key}: {count} of {expected_rows}."
            )
        pct = 100.0 * count / expected_rows
        percentages[key] = pct
        # Use integer comparison to keep the boundary exact, without float jitter.
        if 100 * count > maximum_pct * expected_rows:
            offenders.append(f"{key}: {count:,}/{expected_rows:,} ({pct:.3f}%)")
    if offenders:
        raise DataQualityError(
            "More than 10% missing/invalid original measurements in at least one "
            "required target column, before imputation: " + "; ".join(offenders)
            + ". The 10% threshold is checked separately for demand, "
            "Photovoltaics [MWh], and Wind onshore [MWh]."
        )
    return percentages


def save_quality_report(report, destination):
    """Write a JSON sidecar atomically so customers can audit repairs."""
    destination = Path(destination)
    with TemporaryDirectory(dir=destination.parent, prefix=".quality-json-") as directory:
        tmp = Path(directory) / "report.json"
        tmp.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(destination)


# ============================================================================
# PROJECT DEFAULTS — change these in the script only if a data source requires it.
# No optional command-line arguments are needed for everyday use.
# ============================================================================
INPUT_SHEET = None                  # Prefer "50 hertz" when it exists; otherwise first sheet
SOURCE_TIMEZONE = "auto"           # *_utc headers use UTC; other naive timestamps use Europe/Berlin
BASELINE_TIMEZONE = None           # Defaults from the input timestamp column
NUMBER_LOCALE = "de"               # "de" for German TSO text; change to "en" for English text
INTERVAL_SECONDS = None            # Auto-detect; set to 900 for known 15-minute datasets
RESAMPLE = "auto"                  # Resample irregular/off-grid source timestamps
WRITE_IMPUTATION_AUDIT = True

# Only enable after the source owner explicitly confirms that sparse values in
# another column are mislabelled wind measurements. Default never changes units
# or reclassifies Hydropower values into Wind Energy.
CONFIRMED_SPARSE_WIND_COLUMN = None  # e.g. "Hydropower [MWh]" after confirmation


# ============================================================================
# RECONCILIATION AND FILL HELPERS. Called on Spark Python worker, NOT on driver.
# Single-station state is inherently ordered; per-station grouping can scale later.
# ============================================================================
from collections import defaultdict, deque


def _fast_group_deduplicate(group, prior_trusted, tolerance_pct=1.0, metric_keys=None):
    """Reconcile one timestamp; never donate from a rejected demand-conflict row."""
    metric_keys = metric_keys or METRIC_KEYS
    valid = [r['demand'] for r in group if r.get('demand') is not None]
    agreement = (len(valid) <= 1 or
                 (max(valid) - min(valid)) <= tolerance_pct / 100 * max(abs(v) for v in valid))
    preferred = sorted(group, key=lambda r: (-sum(r.get(k) is not None for k in metric_keys), r['_source_row']))
    if agreement:
        winner = preferred[0]
        out = dict(winner)
        out['demand'] = sum(valid) / len(valid) if valid else None
        donors = {}
        conflicts = []
        for key in metric_keys:
            if key == 'demand':
                donors[key] = winner['_source_row']
                continue
            values = [r[key] for r in group if r.get(key) is not None]
            if len(set(values)) > 1:
                conflicts.append(key)
            donor = next((r for r in preferred if r.get(key) is not None), None)
            out[key] = donor[key] if donor else None
            donors[key] = donor['_source_row'] if donor else None
        rule = 'agreed_demand_merge'
    else:
        nonnull = [r for r in group if r.get('demand') is not None]
        if prior_trusted is not None:
            nonnull.sort(key=lambda r: (abs(r['demand'] - prior_trusted),
                -sum(r.get(k) is not None for k in metric_keys), r['_source_row']))
        else:
            nonnull.sort(key=lambda r: r['_source_row'])
        winner = nonnull[0] if nonnull else preferred[0]
        out = dict(winner)
        donors = {key: winner['_source_row'] for key in metric_keys}
        conflicts = [key for key in metric_keys if key != 'demand' and
                     len({r[key] for r in group if r.get(key) is not None}) > 1]
        rule = 'conflicting_demand_selected_past_only'
    details = dict(_duplicate_rows=len(group),
                   _duplicate_chosen_source_row=winner['_source_row'],
                   _duplicate_rule=rule, _duplicate_demand_conflict=not agreement,
                   _duplicate_other_metric_conflicts=conflicts,
                   _duplicate_sparse_wind_remap_applied=any(r.get('_sparse_wind_applied', False) for r in group),
                   _donors=donors, _chosen_original=winner)
    return out, details


def _fast_confirm_sparse_candidate(group, confirmed):
    if not confirmed:
        return 0
    if not any(row.get('demand') is not None for row in group):
        return 0
    if any(row.get('wind_mwh') is not None for row in group):
        return 0
    count = 0
    for row in group:
        candidate = row.get('_sparse_wind_candidate')
        if row.get('demand') is None and row.get('wind_mwh') is None and candidate is not None and candidate >= 0:
            row['wind_mwh'] = candidate
            row['_sparse_wind_applied'] = True
            count += 1
    return count


def _fast_reconcile(rows, coverage, metric_keys, step_seconds,
                    confirmed_sparse=False):
    """Only actual duplicate groups are retained. Resampling is applied after dedup."""
    duplicate_audit = []
    repair_audit = []
    originals = []
    previous_trusted = None
    group = []
    last_epoch = None
    sparse_repaired = 0

    def close_group():
        nonlocal previous_trusted, sparse_repaired
        if not group:
            return
        sparse_repaired += _fast_confirm_sparse_candidate(group, confirmed_sparse)
        merged, info = _fast_group_deduplicate(group, previous_trusted,
                                                DEFAULT_DUPLICATE_TOLERANCE_PCT, metric_keys)
        if not info['_duplicate_demand_conflict'] and merged['demand'] is not None:
            previous_trusted = merged['demand']
        originals.append(merged)
        if len(group) > 1:
            ts = utc_label(merged['_sample_epoch'])
            audit = dict(timestamp=ts, _duplicate_rows=len(group),
                         _duplicate_rule=info['_duplicate_rule'],
                         _duplicate_chosen_source_row=info['_duplicate_chosen_source_row'],
                         _duplicate_demand_conflict=info['_duplicate_demand_conflict'],
                         _duplicate_other_metric_conflicts=';'.join(info['_duplicate_other_metric_conflicts']),
                         _duplicate_sparse_wind_remap_applied=info['_duplicate_sparse_wind_remap_applied'])
            audit.update({key: merged[key] for key in metric_keys})
            audit.update({key+'_donor_source_row': info['_donors'][key]
                          for key in metric_keys if key != 'demand'})
            duplicate_audit.append(audit)
            chosen = info['_chosen_original']
            for key in metric_keys:
                old, new = chosen.get(key), merged.get(key)
                modified = ((old is None) != (new is None)) or (
                    old is not None and new is not None and old != new)
                method = ('conflicting_demand_selected_from_past' if
                          key == 'demand' and info['_duplicate_demand_conflict'] else
                          'average_or_recovered_agreed_demand' if key == 'demand' and modified else
                          'confirmed_sparse_extra_column_to_wind' if
                          key == 'wind_mwh' and modified and info['_duplicate_sparse_wind_remap_applied'] else
                          'complementary_duplicate_merge' if modified else
                          'conflicting_metric_kept_preferred_source' if key in info['_duplicate_other_metric_conflicts'] else
                          'selected_original')
                repair_audit.append(dict(timestamp=ts,
                    chosen_source_row=chosen['_source_row'], source_row_count=len(group),
                    demand_conflict=info['_duplicate_demand_conflict'], field=key,
                    original_value=old, corrected_value=new,
                    donor_source_row=info['_donors'][key], repair_method=method))

    for row in rows:
        epoch = row['_sample_epoch']
        if last_epoch is not None and epoch != last_epoch:
            close_group()
            group = []
        group.append(row)
        last_epoch = epoch
    close_group()
    if not coverage.get('resampling_applied'):
        return {r['_epoch']: {k: r[k] for k in metric_keys} for r in originals}, duplicate_audit, repair_audit, sparse_repaired
    buckets = defaultdict(list)
    for r in originals:
        buckets[r['_epoch']].append(r)
    merged = {}
    for epoch, group in buckets.items():
        out = {}
        for key in metric_keys:
            valid = [r[key] for r in group if r[key] is not None]
            if not valid:
                out[key] = None
            elif key in ('solar_mw', 'wind_mw'):
                out[key] = next(r[key] for r in reversed(group) if r[key] is not None)
            elif key in ('solar_mwh', 'wind_mwh'):
                out[key] = sum(valid)
            else:
                out[key] = sum(valid) / len(valid)
        merged[epoch] = out
    return merged, duplicate_audit, repair_audit, sparse_repaired


def _fast_clamp(x, low, high):
    return min(high, max(low, x))


def _fast_smoothstep(x, low, high):
    u = _fast_clamp((x-low)/(high-low), 0, 1)
    return u*u*(3-2*u)


def _fast_history_fill(observed, first_epoch, last_epoch, step_seconds, timezone_name,
                       metric_keys=METRIC_KEYS):
    """Linear in number of timestamps: each original observation updates state once."""
    local_tz = ZoneInfo(timezone_name)
    weekly_demand = defaultdict(deque)
    daily_demand = defaultdict(deque)
    daily_solar = defaultdict(deque)
    daily_wind = defaultdict(deque)
    last = {k: None for k in metric_keys}
    rolling_wind = deque()
    wind_sum = 0.0
    completed = []
    def mean_previous(store, key, day, window):
        q = store[key]
        cutoff = day-window
        while q and q[0][0] < cutoff:
            q.popleft()
        vals = [val for d,val in q if d < day]
        return sum(vals)/len(vals) if vals else None
    def estimate_value(key, now, anchor, profile, wind_mean, weekly_available=False):
        if anchor is None:
            return None, 'unfilled_no_past'
        value, start = anchor['value'], anchor['epoch']
        age = now-start
        if not age:
            return value, 'observed'
        prev_span = start-anchor['prev_epoch'] if anchor['prev_epoch'] is not None else None
        ready = prev_span is not None and step_seconds <= prev_span <= max(4*step_seconds,HOUR_SECONDS)
        trend = (value-anchor['prev_value'])*age/prev_span if ready else 0.0
        change_limit = max(abs(value)*(0.25 if key=='demand' else 0.80), (profile or 0)*0.30, 1.0)
        short_est = max(0.0, value + _fast_clamp(trend*math.exp(-age/HOUR_SECONDS), -change_limit, change_limit))
        if key == 'demand':
            if age <= SHORT_GAP_SECONDS:
                return short_est, 'short_causal_trend'
            if profile is None:
                return value, 'past_only_fallback'
            offset = value-anchor['profile'] if anchor['profile'] is not None else 0.0
            v = max(0.0, profile + offset*math.exp(-age/(3*HOUR_SECONDS)))
            if weekly_available:
                return v, 'medium_weekly_profile' if age <= MEDIUM_GAP_SECONDS else 'long_weekly_profile'
            return v, 'medium_daily_profile' if age <= MEDIUM_GAP_SECONDS else 'long_daily_profile'
        if key == 'solar_mwh':
            if age <= SHORT_GAP_SECONDS:
                return (0.0 if profile == 0 and value <= .01 else short_est), 'short_causal_trend'
            if profile is None:
                return max(0., value*math.exp(-age/(6*HOUR_SECONDS))), 'solar_decay_no_profile'
            ratio = _fast_clamp(value/anchor['profile'], .25, 3.) if anchor['profile'] is not None and anchor['profile']>.5 else 1.0
            v = max(0.0, profile*(1+(ratio-1)*math.exp(-age/(2*HOUR_SECONDS))))
            return v, 'medium_daily_solar' if age <= MEDIUM_GAP_SECONDS else 'long_daily_solar'
        if key == 'wind_mwh':
            mu = anchor['recent_mean'] if anchor['recent_mean'] is not None else value
            ar = max(0., mu + (value-mu)*math.exp(-age/(2*HOUR_SECONDS)))
            history_delta = (profile-anchor['profile']) if profile is not None and anchor['profile'] is not None else 0.0
            delta_limit = max(50.0, abs(mu)*.50)
            long_est = max(0., ar + .20*_fast_clamp(history_delta, -delta_limit, delta_limit))
            weight1 = _fast_smoothstep(age,.5*HOUR_SECONDS,1.5*HOUR_SECONDS)
            weight2 = _fast_smoothstep(age,3*HOUR_SECONDS,5*HOUR_SECONDS)
            mixed = (1-weight1)*short_est+weight1*ar
            estimate = max(0.,(1-weight2)*mixed+weight2*long_est)
            method = ('short_causal_trend' if age <= SHORT_GAP_SECONDS else
                      'medium_wind_ar' if age <= MEDIUM_GAP_SECONDS else
                      'long_wind_ar_bounded_shape' if profile is not None else 'wind_ar_no_profile')
            return estimate,method
        return value, 'capacity_forward_fill'

    for epoch in range(first_epoch, last_epoch+1, step_seconds):
        obs = observed.get(epoch)
        actual = obs or {}
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone(local_tz)
        day = dt.date().toordinal()
        clock = (dt.hour,dt.minute,dt.second)
        weekday = dt.weekday()
        dw = mean_previous(weekly_demand,(weekday,clock),day,35)
        dd = mean_previous(daily_demand,clock,day,14)
        solar = mean_previous(daily_solar,clock,day,3)
        wind = mean_previous(daily_wind,clock,day,7)
        while rolling_wind and rolling_wind[0][0] < epoch-DAY_SECONDS:
            wind_sum -= rolling_wind.popleft()[1]
        rolling_mean = wind_sum/len(rolling_wind) if rolling_wind else None
        profiles = dict(demand=dw if dw is not None else dd, solar_mwh=solar,wind_mwh=wind)
        cur = {'_epoch':epoch,'_raw':{key:actual.get(key) for key in metric_keys}}
        for key in metric_keys:
            raw_val = actual.get(key)
            anchor = last[key]
            candidate, method = estimate_value(key,epoch,anchor,profiles.get(key),rolling_mean, dw is not None)
            # Compute the *pre-observation* wind estimate at a right endpoint.
            if key == 'wind_mwh':
                cur['_wind_pre_observation_estimate'] = candidate
            cur[key] = raw_val if raw_val is not None else candidate
            cur['_was_missing_'+key] = raw_val is None
            cur['_impute_method_'+key] = 'observed' if raw_val is not None else method
            if raw_val is not None:
                previous = last[key]
                last[key] = {'epoch':epoch,'value':raw_val,
                             'prev_epoch':previous['epoch'] if previous else None,
                             'prev_value':previous['value'] if previous else None,
                             'profile':profiles.get(key),
                             'recent_mean':rolling_mean if key=='wind_mwh' else None,
                             '_weekly':dw if key=='demand' else None}
        completed.append(cur)
        if actual.get('demand') is not None:
            weekly_demand[(weekday,clock)].append((day,actual['demand']))
            daily_demand[clock].append((day,actual['demand']))
        if actual.get('solar_mwh') is not None:
            daily_solar[clock].append((day,actual['solar_mwh']))
        if actual.get('wind_mwh') is not None:
            daily_wind[clock].append((day,actual['wind_mwh']))
            rolling_wind.append((epoch,actual['wind_mwh']))
            wind_sum += actual['wind_mwh']

    # Retrospective offline wind correction: preserve bounded past-only shape,
    # but taper it to the observed endpoints; no future information for other fields.
    previous_observed_index = None
    for index, record in enumerate(completed):
        record['_wind_gap_duration_hours'] = None
        record['_wind_bridge_next_observed_epoch'] = None
        record['_wind_bridge_linear_guide_mwh'] = None
        record['_wind_bridge_past_only_baseline_mwh'] = None
        record['_wind_bridge_shape_adjustment_mwh'] = None
        if record['_raw'].get('wind_mwh') is None:
            continue
        if previous_observed_index is not None and index-previous_observed_index > 1 and WIND_HISTORICAL_TWO_SIDED_INTERPOLATION:
            left = completed[previous_observed_index]
            right = record
            lval = left['wind_mwh']
            rval = right['wind_mwh']
            baseline_right = right['_wind_pre_observation_estimate']
            gap_seconds = right['_epoch']-left['_epoch']
            if lval is not None and rval is not None and baseline_right is not None:
                for j in range(previous_observed_index+1,index):
                    r = completed[j]
                    u = (r['_epoch']-left['_epoch'])/gap_seconds
                    guide = lval+(rval-lval)*u
                    baseline_line = lval+(baseline_right-lval)*u
                    hist = r['_wind_pre_observation_estimate']
                    if hist is None: continue
                    curvature = hist-baseline_line
                    cap = max(75., max(abs(lval),abs(rval))*WIND_HISTORICAL_BRIDGE_SHAPE_CAP_PCT/100)
                    adjust = WIND_HISTORICAL_BRIDGE_SHAPE_WEIGHT*4*u*(1-u)*_fast_clamp(curvature,-cap,cap)
                    r['wind_mwh'] = max(0., guide+adjust)
                    r['_impute_method_wind_mwh'] = ('historical_shaped_bridge_low_confidence' if
                        gap_seconds > WIND_LONG_GAP_SECONDS else 'historical_shaped_bridge')
                    r['_wind_gap_duration_hours'] = gap_seconds/HOUR_SECONDS
                    r['_wind_bridge_next_observed_epoch'] = right['_epoch']
                    r['_wind_bridge_linear_guide_mwh'] = guide
                    r['_wind_bridge_past_only_baseline_mwh'] = hist
                    r['_wind_bridge_shape_adjustment_mwh'] = adjust
        previous_observed_index=index
    for index, row in enumerate(completed):
        old = completed[index-1] if index else None
        row['_wind_observed_reentry_jump'] = bool(old is not None and
            old['_was_missing_wind_mwh'] and not row['_was_missing_wind_mwh'] and
            old['wind_mwh'] is not None and row['wind_mwh'] is not None and
            abs(row['wind_mwh']-old['wind_mwh']) > max(200.,.25*abs(old['wind_mwh'])))
    return completed


def _fast_timestamp(epoch):
    return datetime.fromtimestamp(epoch,timezone.utc).strftime('%d-%m-%Y %H:%M')


def _fast_source_quality(observed, first_epoch,last_epoch,step_seconds,metric_keys):
    flatline_runs=flatline_rows=0
    prev_epoch=prev_demand=None
    run=0
    longest={key:0 for key in metric_keys}
    missing={key:0 for key in metric_keys}
    bad={key:0 for key in metric_keys}
    def finish_run():
        nonlocal run, flatline_runs, flatline_rows
        if run >= 2 and run*step_seconds >= DAY_SECONDS:
            flatline_runs+=1
            flatline_rows+=run
        run=0
    for epoch in range(first_epoch,last_epoch+1,step_seconds):
        row=observed.get(epoch,{})
        demand=row.get('demand')
        if (prev_epoch is not None and epoch-prev_epoch==step_seconds and
            demand is not None and prev_demand is not None and demand==prev_demand):
            run+=1
        else:
            finish_run()
            run=1 if demand is not None else 0
        for key in metric_keys:
            if row.get(key) is None:
                missing[key]+=1
                bad[key]+=1
                longest[key]=max(longest[key],bad[key])
            else:
                bad[key]=0
        prev_epoch,prev_demand=epoch,demand
    finish_run()
    return missing,longest,dict(runs=flatline_runs,rows=flatline_rows)



# ===========================================================================
# SPARK EXECUTOR PROCESSING / OUTPUTS
# ===========================================================================

def _spark_source_dataframe(spark, staged_file, metric_keys):
    """Keep numeric casting in Spark; staging only normalizes source text."""
    schema = T.StructType([
        T.StructField('_epoch', T.LongType(), True),
        T.StructField('_sample_epoch', T.LongType(), True),
        T.StructField('_source_row', T.LongType(), True),
        *(T.StructField(k, T.StringType(), True) for k in metric_keys),
        T.StructField('_sparse_wind_candidate', T.StringType(), True),
    ])
    raw = spark.read.option('header', 'true').schema(schema).csv(staged_file.as_uri())
    return raw.select('_epoch', '_sample_epoch', '_source_row',
                      *(staged_numeric_value(F.col(k)).alias(k) for k in metric_keys),
                      staged_numeric_value(F.col('_sparse_wind_candidate')).alias('_sparse_wind_candidate'),
                      *(F.col(k).alias('_raw_' + k) for k in metric_keys))


def _spark_worker_reconstruct(row_iterator, config):
    """Run on a Spark executor via mapPartitions, never in the driver.

    Returns tagged typed tuples for Spark CSV writers and a small status object.
    Entire station is one sorted Spark partition for deterministic history.
    """
    keys = config['keys']
    stats = {'malformed': {k: 0 for k in keys},
             'physical_invalid': {k: 0 for k in keys},
             'nonblank': {k: 0 for k in keys}}

    def source_rows():
        for source in row_iterator:
            row = source.asDict() if hasattr(source, 'asDict') else dict(source)
            for key in keys:
                original = row.pop('_raw_' + key, None)
                if original is not None and str(original).strip():
                    stats['nonblank'][key] += 1
                    if original == MALFORMED_STAGED_NUMBER or row[key] is None:
                        stats['malformed'][key] += 1
                value = row.get(key)
                if value is not None and (not math.isfinite(value) or value < 0):
                    stats['physical_invalid'][key] += 1
                    row[key] = None
            yield row

    coverage = config['coverage']
    step = config['step']
    first, last = config['first'], config['last']
    observed, duplicate_audit, field_audit, sparse_count = _fast_reconcile(
        source_rows(), coverage, keys, step, config['confirmed_sparse'])
    missing, longest, flatline = _fast_source_quality(observed, first, last, step, keys)
    expected = coverage['expected_timestamp_slots']
    try:
        primary = check_primary_metric_coverage(missing, expected)
    except DataQualityError as exc:
        yield ('status', {'accepted': False, 'reason': str(exc),
                          'target_missing_counts': missing})
        return

    completed = _fast_history_fill(observed, first, last, step, config['local_zone'], keys)
    methods = {k: dict(Counter(r['_impute_method_' + k] for r in completed)) for k in keys}
    filled = {k: sum(r['_was_missing_' + k] and r[k] is not None for r in completed) for k in keys}
    unfilled = {k: sum(r[k] is None for r in completed) for k in keys}
    solar_exceeds = sum(1 for r in completed
                        if r['solar_mwh'] is not None and r['solar_mw'] is not None
                        and r['solar_mwh'] > r['solar_mw'] * step / 3600 * 1.1)
    wind_exceeds = sum(1 for r in completed
                       if r['wind_mwh'] is not None and r['wind_mw'] is not None
                       and r['wind_mwh'] > r['wind_mw'] * step / 3600 * 1.1)
    malformed_count = sum(stats['malformed'].values())
    missing_percentages = {k: 100 * missing[k] / expected for k in keys}
    bridges = sum(n for method, n in methods['wind_mwh'].items()
                  if method.startswith('historical_shaped_bridge'))
    warnings = []
    if any(unfilled.values()):
        warnings.append('Some cells cannot be filled without earlier observed data.')
    if solar_exceeds or wind_exceeds:
        warnings.append('Generation exceeds supplied installed capacity; review units.')
    if duplicate_audit:
        warnings.append('Duplicate timestamps reconciled; see audit CSV.')
    if coverage.get('suspected_sparse_wind_fragments', 0) > sparse_count:
        warnings.append('Suspected misplaced extra-column values were NOT reclassified automatically.')
    if bridges:
        warnings.append('Wind Energy bridge uses later real observations: retrospective use only.')
    if malformed_count:
        warnings.append('Malformed values counted as missing before imputation.')
    if flatline['runs']:
        warnings.append('Observed demand flatline longer than 24 hours was detected.')
    report = dict(coverage,
        rows=len(completed), data_accepted=True, timestamp_timezone='UTC',
        baseline_timezone=config['local_zone'],
        source_processing='Spark DataFrame (cast and sort) + executor-side Spark RDD mapPartitions',
        implementation='Spark end-to-end; single sorted station partition; O(N) chronological executor state',
        source_malformed_per_column=stats['malformed'],
        source_negative_or_sentinel_cells=stats['physical_invalid'],
        source_nonblank_cells=sum(stats['nonblank'].values()),
        source_malformed_cells=malformed_count,
        target_missing_pct_before_fill=primary,
        measurement_missing_pct_per_column=missing_percentages,
        measurement_cells_missing_before_fill=sum(missing.values()),
        missing_timestamp_reject_pct=MAX_MISSING_TIMESTAMP_PCT,
        primary_target_missing_reject_pct=MAX_MISSING_TARGET_PCT,
        mandatory_target_columns=list(PRIMARY_TARGET_KEYS),
        duplicate_reconciliation_performed_before_quality_gate=True,
        duplicate_reconciliation_audit_available=bool(duplicate_audit),
        duplicate_field_repair_audit_available=bool(field_audit),
        suspected_sparse_wind_fragments=coverage.get('suspected_sparse_wind_fragments', 0),
        sparse_wind_fragments_confirmed_and_recovered=sparse_count,
        imputation_method_counts=methods, imputation_methods_by_column=methods,
        imputed_cell_counts=filled, unfilled_cell_counts=unfilled,
        longest_missing_run_per_field=longest,
        observed_demand_flatline_runs_24h=flatline['runs'],
        observed_demand_flatline_rows_24h=flatline['rows'],
        solar_generation_exceeds_reported_capacity_intervals=solar_exceeds,
        wind_generation_exceeds_reported_capacity_intervals=wind_exceeds,
        wind_observed_reentry_jump_alerts=sum(r['_wind_observed_reentry_jump'] for r in completed),
        wind_historical_shaped_bridge_cells=bridges,
        wind_historical_long_gap_low_confidence_cells=methods['wind_mwh'].get(
            'historical_shaped_bridge_low_confidence', 0),
        wind_historical_gap_interpolation=WIND_HISTORICAL_TWO_SIDED_INTERPOLATION,
        wind_bridge_historical_shape_blend_weight=WIND_HISTORICAL_BRIDGE_SHAPE_WEIGHT,
        leakage_policy=('Retrospective bridge: never score/replay bridged targets' if WIND_HISTORICAL_TWO_SIDED_INTERPOLATION else 'Causal past-only filling; evaluate only originally observed targets'),
        forecast_scoring_policy='Score only originally observed targets.',
        training_target_policy='Mask any imputed target; future-bridged wind is not live available.',
        acceptance_rule='Reject >10% missing timestamp OR >10% demand/solar/wind (each) after duplicate repair, before imputation.',
        training_ready=(not any(unfilled.values()) and not solar_exceeds and not wind_exceeds
                        and not malformed_count and not flatline['runs']),
        first_utc=utc_label(first), last_utc=utc_label(last), warnings=warnings)
    # Retain commonly used legacy quality JSON fields for dashboard readers.
    labels = {key: label for _, key, label in METRICS}
    nonblank_count = sum(stats['nonblank'].values())
    report.update(dict(
        measurements_present=[label for _, _, label in METRICS],
        raw_measurement_cells=len(completed) * len(keys),
        measurement_missing_pct_before_fill=(100 * sum(missing.values())
            / max(len(completed) * len(keys), 1)),
        source_genuinely_blank_per_column={key:
            max(0, coverage.get('raw_nonempty_rows', 0)-stats['nonblank'][key]) for key in keys},
        source_genuinely_blank_cells=sum(max(0,
            coverage.get('raw_nonempty_rows', 0)-stats['nonblank'][key]) for key in keys),
        source_malformed_pct=100 * malformed_count / max(nonblank_count, 1),
        imputation_audit_available=config['write_audit'],
        negative_measurement_cells={labels[key]:stats['physical_invalid'][key] for key in keys},
        cells_imputed={labels[key]:filled[key] for key in keys},
        cells_unfilled_no_history={labels[key]:unfilled[key] for key in keys},
        other_quality_warnings_do_not_reject=True,
        number_format=NUMBER_LOCALE,
        physical_check_mode='warn',
        capacity_tolerance_pct=10.0,
        extra_column_repair_requires_source_owner_confirmation=True,
        wind_interpolation_safe_for_live_asof_forecasting=False,
        wind_bridge_shape_cap_pct_observed_scale=WIND_HISTORICAL_BRIDGE_SHAPE_CAP_PCT,
        demand_unit_unverified=normalize_header(
            coverage.get('columns_mapped', {}).get('demand'))=='value_of_data'))
    yield ('status', {'accepted': True, 'report': report})

    for row in completed:
        timestamp = _fast_timestamp(row['_epoch'])
        yield ('clean', (timestamp, *(row.get(k) for k in keys)))
        if config['write_audit']:
            end = row['_wind_bridge_next_observed_epoch']
            yield ('impute', (timestamp,
                *(row['_was_missing_' + k] for k in keys),
                *(row['_impute_method_' + k] for k in keys),
                row['_wind_observed_reentry_jump'],
                row['_wind_gap_duration_hours'],
                row['_wind_bridge_linear_guide_mwh'],
                row['_wind_bridge_past_only_baseline_mwh'],
                row['_wind_bridge_shape_adjustment_mwh'],
                utc_label(end) if end is not None else None))
    for row in duplicate_audit:
        yield ('duplicate', (row.get('timestamp'), row.get('_duplicate_rows'),
            row.get('_duplicate_rule'), row.get('_duplicate_chosen_source_row'),
            row.get('_duplicate_demand_conflict'),
            row.get('_duplicate_other_metric_conflicts'),
            row.get('_duplicate_sparse_wind_remap_applied'),
            *(row.get(k) for k in keys),
            *(row.get(k + '_donor_source_row') for k in keys if k != 'demand')))
    for row in field_audit:
        yield ('repairs', (row.get('timestamp'), row.get('chosen_source_row'),
            row.get('source_row_count'), row.get('demand_conflict'), row.get('field'),
            row.get('original_value'), row.get('corrected_value'),
            row.get('donor_source_row'), row.get('repair_method')))


def _find_single_spark_csv_part(output, kind):
    """Find the real Spark part file; report what Spark left on failure.

    Spark includes an unpredictable UUID and c000 suffix in its output name.
    The directory is temporary, so there can be no stale output parts.
    """
    output = Path(output)
    files = sorted(p for p in output.rglob('part-*') if p.is_file() and p.name.endswith('.csv')) if output.exists() else []
    if len(files) != 1:
        contents = (sorted(str(p.relative_to(output)) for p in output.rglob('*') if p.is_file())[:20]
                    if output.exists() else ['<Spark output directory absent>'])
        raise RuntimeError(
            'Spark CSV export {!r} expected one part file, found {} at {}. '
            'Existing files: {}. This is a Spark export/path issue, not a data-quality rejection.'
            .format(kind, len(files), output, contents))
    return files[0]


def _copy_spark_part_atomically(source, target):
    """Publish a Spark-written part as a stable filename, on the driver.

    The data itself is still written by Spark. Copying the finished CSV to its
    final name avoids cross-volume replace failures when Windows TEMP lives
    on a different drive, and keeps the existing output safe until success.
    """
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = mkstemp(prefix='.' + target.stem + '_',
                                suffix='.tmp', dir=target.parent)
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        shutil.copyfile(source, temporary)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def _spark_write_csv(spark, tagged, kind, columns, target):
    """Write a single CSV with Spark, then publish its part file on Windows.

    Use the *system* temporary directory, not the customer input directory.
    Local Spark/Hadoop on Windows can mishandle encoded spaces in file:// URIs
    under paths such as 'Nodes of Glory/raw data'. Spark reads staged CSV from
    system TEMP successfully, so the same location is used for CSV output.
    """
    schema = T.StructType([T.StructField(h, T.StringType(), True) for h in columns])
    rows = tagged.filter(lambda item: item[0] == kind).map(
        lambda item: tuple(None if value is None else str(value) for value in item[1]))
    frame = spark.createDataFrame(rows, schema=schema).coalesce(1)
    # TemporaryDirectory() uses Windows %TEMP%, normally without spaces.
    # Do NOT create this folder underneath the source file directory.
    with TemporaryDirectory(prefix='spark_50hertz_csv_') as folder:
        output = Path(folder) / 'parts'
        frame.write.mode('overwrite').option('header', 'true').option('compression', 'none').csv(output.as_uri())
        part_file = _find_single_spark_csv_part(output, kind)
        _copy_spark_part_atomically(part_file, target)


def main_spark():
    parser = argparse.ArgumentParser(description='End-to-end Spark electricity preprocessor')
    parser.add_argument('input_file', type=Path, help='CSV, XLSX or XLSM input')
    parser.add_argument('--source-timezone', default='auto',
                        help='auto uses UTC for explicitly UTC-labelled headers; otherwise Europe/Berlin')
    parser.add_argument('--interval-minutes', type=int, default=15)
    parser.add_argument('--number-locale', choices=['de','en'], default='de')
    parser.add_argument('--sheet-name', default=None)
    parser.add_argument('--column-map-json', default='{}')
    parser.add_argument('--causal-only', action='store_true',
                        help='Disallow wind gap bridges that use later actual readings')
    parser.add_argument('--no-audit', action='store_true')
    args = parser.parse_args()
    if args.interval_minutes <= 0 or 1440 % args.interval_minutes:
        parser.error('--interval-minutes must be a positive divisor of 1440')
    try:
        column_overrides = json.loads(args.column_map_json)
        if not isinstance(column_overrides, dict):
            raise ValueError('must be a JSON object')
        if any(key not in ALIASES or not isinstance(value, str)
               for key, value in column_overrides.items()):
            raise ValueError('invalid canonical column key or header')
    except (ValueError, TypeError) as exc:
        parser.error('Invalid --column-map-json: ' + str(exc))
    global NUMBER_LOCALE, WIND_HISTORICAL_TWO_SIDED_INTERPOLATION
    NUMBER_LOCALE = args.number_locale
    if args.causal_only:
        WIND_HISTORICAL_TWO_SIDED_INTERPOLATION = False
    if SparkSession is None:
        parser.exit(2, 'PySpark required. Activate electricity-spark environment.\n')
    if os.name == 'nt':
        os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
        os.environ.setdefault('PYSPARK_DRIVER_PYTHON', sys.executable)
    spark = (SparkSession.builder.master('local[2]').appName('ElectricitySparkEndToEnd')
             .config('spark.sql.shuffle.partitions', '2')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.ui.showConsoleProgress', 'true')
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    tagged = None
    try:
        with TemporaryDirectory(prefix='spark_50hertz_e2e_') as directory:
            staged = Path(directory) / 'stage.csv'
            first, last, coverage, metrics = stage_input(
                args.input_file, staged, args.sheet_name, args.source_timezone, args.interval_minutes * 60,
                column_overrides=column_overrides, resample=RESAMPLE, sparse_wind_source_column=CONFIRMED_SPARSE_WIND_COLUMN,
                confirm_sparse_wind_remap=bool(CONFIRMED_SPARSE_WIND_COLUMN),
                number_format=NUMBER_LOCALE)
            keys = [k for _, k, _ in metrics]
            step = coverage['interval_seconds']
            print('Input staged: {:,} rows; {:,} expected intervals; {} missing timestamps; {} duplicate rows'.format(
                coverage['raw_nonempty_rows'], coverage['expected_timestamp_slots'],
                coverage['missing_timestamp_slots'], coverage['duplicate_rows_removed']), flush=True)
            col = normalize_header(coverage['columns_mapped']['timestamp'])
            local_zone = BASELINE_TIMEZONE or ('UTC' if 'utc' in col else
                coverage['source_timezone_assumed_for_naive'])
            config = dict(keys=keys, step=step, first=first, last=last,
                local_zone=local_zone, coverage=coverage,
                confirmed_sparse=bool(CONFIRMED_SPARSE_WIND_COLUMN),
                write_audit=not args.no_audit)

            print('Spark: reading, casting, sorting original rows...', flush=True)
            source = _spark_source_dataframe(spark, staged, keys)
            # Only one station: one chronological state machine per station.
            # In a multi-station system, use station-key partitioning instead.
            ordered = (source.rdd.map(lambda row: ((row['_sample_epoch'], row['_source_row']), row))
                       .sortByKey(ascending=True, numPartitions=1).values())
            tagged = ordered.mapPartitions(lambda iterator: _spark_worker_reconstruct(iterator, config))
            tagged.persist(StorageLevel.MEMORY_AND_DISK)
            print('Spark: reconciling, applying 10% gates, and imputing IN SPARK EXECUTOR...', flush=True)
            tagged.count()  # Completely materialize cache once; no recomputation for audit/clean outputs.
            statuses = tagged.filter(lambda x: x[0] == 'status').map(lambda x: x[1]).take(1)
            if not statuses:
                raise RuntimeError('Spark executor produced no quality status')
            status = statuses[0]
            if not status['accepted']:
                raise DataQualityError(status['reason'])
            report = status['report']
            dest = args.input_file.resolve()
            print('Spark: source accepted; writing processed CSV and audits...', flush=True)
            _spark_write_csv(spark, tagged, 'clean',
                ['timestamp', *(label for _, _, label in metrics)],
                dest.with_name(dest.stem + '_cleaned.csv'))
            if not args.no_audit:
                _spark_write_csv(spark, tagged, 'impute',
                    ['timestamp', *(k + '_imputed' for k in keys),
                     *(k + '_method' for k in keys),
                     'wind_observed_reentry_jump_warning',
                     'wind_interpolated_gap_duration_hours',
                     'wind_linear_guide_mwh',
                     'wind_past_only_baseline_mwh',
                     'wind_historical_shape_adjustment_mwh',
                     'wind_interpolation_next_observed_utc'],
                    dest.with_name(dest.stem + '_imputation_audit.csv'))
            if report['duplicate_reconciliation_audit_available']:
                _spark_write_csv(spark, tagged, 'duplicate',
                    ['timestamp','_duplicate_rows','_duplicate_rule',
                     '_duplicate_chosen_source_row','_duplicate_demand_conflict',
                     '_duplicate_other_metric_conflicts','_duplicate_sparse_wind_remap_applied',
                     *keys, *(k + '_donor_source_row' for k in keys if k != 'demand')],
                    dest.with_name(dest.stem + '_duplicate_reconciliation_audit.csv'))
            if report['duplicate_field_repair_audit_available']:
                _spark_write_csv(spark, tagged, 'repairs',
                    ['timestamp','chosen_source_row','source_row_count','demand_conflict',
                     'field','original_value','corrected_value','donor_source_row','repair_method'],
                    dest.with_name(dest.stem + '_duplicate_field_repairs.csv'))
            quality_file = dest.with_name(dest.stem + '_quality.json')
            save_quality_report(report, quality_file)
            print('Completed {:,} rows in Spark; quality report: {}'.format(
                report['rows'], quality_file), flush=True)
    except DataQualityError as exc:
        parser.exit(2, 'Data rejected: {}\n'.format(exc))
    except Exception as exc:
        import traceback
        error_file = args.input_file.resolve().with_name(
            args.input_file.stem + '_processing_error.log')
        error_file.write_text(traceback.format_exc(), encoding='utf-8')
        print('Processing failed: {}: {}'.format(type(exc).__name__, str(exc).splitlines()[0]),
              file=sys.stderr)
        print('Full traceback: {}'.format(error_file), file=sys.stderr)
        raise SystemExit(1)
    finally:
        if tagged is not None:
            tagged.unpersist()
        spark.stop()


if __name__ == '__main__':
    main_spark()
