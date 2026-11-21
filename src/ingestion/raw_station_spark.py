"""Clean an uploaded station export with the supplied Spark batch rules."""

import argparse
import csv
import json
import math
import os
import re
import sys
from collections import Counter
from contextlib import ExitStack, contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory, TemporaryFile
from urllib.parse import unquote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException
from zipfile import BadZipFile
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F, types as T

HOUR_SECONDS = 60 * 60
DAY_SECONDS = 24 * HOUR_SECONDS
MAX_MISSING_TIMESTAMP_PCT = 20
OUTPUT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"
DEFAULT_DUPLICATE_TOLERANCE_PCT = 1.0

# Workbook header -> internal Spark column -> final output column.
METRICS = (
    ("Demand", "demand", "demand"),
    ("Photovoltaics [MWh]", "solar_mwh", "Photovoltaics [MWh]"),
    ("Photovoltaics_installed_capacity [MW]", "solar_mw", "Photovoltaics_installed_capacity [MW]"),
    ("Wind onshore [MWh]", "wind_mwh", "Wind onshore [MWh]"),
    ("Wind_onshore_installed_capacity [MW]", "wind_mw", "Wind_onshore_installed_capacity [MW]"),
)
METRIC_KEYS = [key for _, key, _ in METRICS]
COLUMN_ALIASES = {
    "timestamp": ("Datum von", "utc_timestamp", "Timestamp", "Datetime", "Date time", "Start date", "Start time"),
    "demand": ("Demand", "Demand [MWh]", "Demand [MW]", "Load", "Load [MWh]", "Load [MW]", "Netzlast [MWh]"),
    "solar_mwh": ("Photovoltaics [MWh]", "Solar [MWh]", "Solar generation [MWh]"),
    "solar_mw": ("Photovoltaics_installed_capacity [MW]", "Solar capacity [MW]", "Solar installed capacity [MW]"),
    "wind_mwh": ("Wind onshore [MWh]", "Wind Energy [MWh]", "Wind generation [MWh]"),
    "wind_mw": ("Wind_onshore_installed_capacity [MW]", "Wind capacity [MW]", "Wind installed capacity [MW]"),
}
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


def parse_local_time(value, row_number, timestamp_format):
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
            if timestamp_format != "auto":
                try:
                    aware = datetime.fromisoformat(value)
                except ValueError:
                    aware = None
                if aware is not None and aware.tzinfo is not None:
                    return aware
                return datetime.strptime(value, timestamp_format)
            iso_value = value[:-1] + "+00:00" if value.endswith("Z") else value
            try:
                return datetime.fromisoformat(iso_value)
            except ValueError:
                pass
            for date_format in ("%d.%m.%Y", "%d-%m-%Y", "%d/%m/%Y"):
                for time_format in (" %H:%M:%S", " %H:%M", ""):
                    try:
                        return datetime.strptime(value, date_format + time_format)
                    except ValueError:
                        continue
    except (KeyError, ValueError):
        pass
    raise DataQualityError(f"Source row {row_number}: invalid timestamp {value!r}.")


def utc_candidates(local_time, zone, row_number):
    """Resolve a wall-clock time to one or two real instants."""
    if local_time.microsecond:
        raise DataQualityError(f"Source row {row_number}: timestamps must have whole-second precision.")
    if local_time.tzinfo is not None:
        return (int(local_time.timestamp()),)

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
            f"Source row {row_number}: {local_time} does not exist in {zone.key}."
        )
    return tuple(sorted(epochs))


def is_blank(value):
    return value is None or (isinstance(value, str) and not value.strip())


def resolve_repeated_hour(block, previous_epoch, next_epoch):
    """Use the order of the two fall-back passes to disambiguate local times."""
    known = [epochs for epochs, _, _ in block if epochs]
    early_ordered = [True]
    previous = previous_epoch
    for early, _ in known:
        early_ordered.append(early_ordered[-1] and (previous is None or previous <= early))
        previous = early

    late_ordered = [True] * (len(known) + 1)
    following = next_epoch
    for index in range(len(known) - 1, -1, -1):
        late = known[index][1]
        late_ordered[index] = late_ordered[index + 1] and (following is None or late <= following)
        following = late

    possibilities = []
    for split in range(len(known) + 1):
        left = known[split - 1][0] if split else previous_epoch
        right = known[split][1] if split < len(known) else next_epoch
        if early_ordered[split] and late_ordered[split] and (left is None or right is None or left <= right):
            possibilities.append(split)
            if len(possibilities) > 1:
                break
    if len(possibilities) != 1:
        raise DataQualityError(
            f"Source row {block[0][1]}: the repeated local hour is ambiguous. "
            "Supply both passes in chronological order, or use unique UTC source timestamps "
            "and select UTC as the source timezone."
        )
    candidates = iter(epochs[int(index >= possibilities[0])] for index, epochs in enumerate(known))
    return [(next(candidates) if epochs else None, row_number, values) for epochs, row_number, values in block]


def timed_source_rows(rows, timestamp_index, metric_indices, zone, timestamp_format):
    """Yield (UTC epoch, worksheet row, raw metrics) in source order."""
    repeated_hour = []
    previous_epoch = None
    for row_number, row in enumerate(rows, start=2):
        values = tuple(row[index] if index is not None and index < len(row) else None
                       for index in metric_indices)
        raw_time = row[timestamp_index] if timestamp_index < len(row) else None
        if is_blank(raw_time) and all(is_blank(value) for value in values):
            continue
        if is_blank(raw_time):
            if repeated_hour:
                repeated_hour.append((None, row_number, values))
            else:
                yield None, row_number, values
            continue

        local_time = parse_local_time(raw_time, row_number, timestamp_format)
        epochs = utc_candidates(local_time, zone, row_number)
        if len(epochs) == 2:
            repeated_hour.append((epochs, row_number, values))
            continue
        epoch = epochs[0]
        if repeated_hour:
            yield from resolve_repeated_hour(repeated_hour, previous_epoch, epoch)
            repeated_hour = []
        yield epoch, row_number, values
        previous_epoch = epoch
    if repeated_hour:
        yield from resolve_repeated_hour(repeated_hour, previous_epoch, None)


def utc_label(epoch):
    pattern = "%d-%m-%Y %H:%M:%S UTC" if epoch % 60 else "%d-%m-%Y %H:%M UTC"
    return datetime.fromtimestamp(epoch, timezone.utc).strftime(pattern)


def infer_timed_rows(timed_rows, buffer):
    """Buffer the parsed workbook while finding the most common unique timestamp spacing."""
    writer = csv.writer(buffer)
    observed = set()
    for epoch, row_number, values in timed_rows:
        writer.writerow([epoch, row_number, *values])
        if epoch is not None:
            observed.add(epoch)
    if not observed:
        raise DataQualityError("No timestamped data rows found.")
    if len(observed) == 1:
        raise DataQualityError("One unique timestamp cannot establish an interval. Set --interval explicitly.")

    ordered = sorted(observed)
    counts = Counter(right - left for left, right in zip(ordered, ordered[1:]))
    step_seconds = min(counts, key=lambda step: (-counts[step], step))
    buffer.seek(0)
    rows = ((int(epoch) if epoch else None, int(row_number), values)
            for epoch, row_number, *values in csv.reader(buffer))
    return step_seconds, rows


def normalized_header(value):
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")


def resolve_headers(header, required_keys, mapping):
    indexes = {}
    for key in required_keys:
        names = (mapping[key],) if key in mapping else COLUMN_ALIASES[key]
        accepted = {normalized_header(name) for name in names}
        matches = [index for index, name in enumerate(header) if normalized_header(name) in accepted]
        if not matches:
            raise DataQualityError(f"Missing source column for {key}: {', '.join(names)}. Map the header in the portal.")
        if len(matches) != 1:
            raise DataQualityError(f"Multiple source columns match {key}. Map one unique header in the portal.")
        indexes[key] = matches[0]
    if len(set(indexes.values())) != len(indexes):
        raise DataQualityError("Each source field must map to a different column.")
    return indexes


@contextmanager
def source_rows(path, sheet_name, required_keys, mapping):
    if path.suffix.lower() == ".csv":
        if sheet_name:
            raise DataQualityError("A worksheet name applies only to Excel uploads.")
        with path.open(encoding="utf-8-sig", newline="") as handle:
            sample = handle.read(8192)
            handle.seek(0)
            try:
                delimiter = csv.Sniffer().sniff(sample, delimiters=",;\t").delimiter
            except csv.Error:
                header_line = sample.splitlines()[0] if sample else ""
                delimiter = max(",;\t", key=lambda item: header_line.count(item))
            rows = csv.reader(handle, delimiter=delimiter)
            yield rows, resolve_headers(next(rows, ()), required_keys, mapping), None
        return

    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        if sheet_name and sheet_name not in workbook.sheetnames:
            raise DataQualityError(f"Sheet {sheet_name!r} is missing. Available: {workbook.sheetnames}.")
        candidates = []
        errors = []
        for name in ([sheet_name] if sheet_name else workbook.sheetnames):
            # Close the header probe so the ZIP stream never stays open.
            rows = workbook[name].values
            try:
                candidates.append((name, resolve_headers(next(rows, ()), required_keys, mapping)))
            except DataQualityError as exc:
                errors.append(f"{name}: {exc}")
            finally:
                rows.close()
        if not candidates:
            raise DataQualityError("No matching worksheet. " + " ".join(errors[:3]))
        if len(candidates) > 1:
            raise DataQualityError("Multiple worksheets match. Enter the worksheet name in the portal.")
        name, indexes = candidates[0]
        rows = workbook[name].values
        try:
            next(rows, None)
            yield rows, indexes, name
        finally:
            rows.close()
    finally:
        workbook.close()


def stage_source(path, csv_path, sheet_name, source_timezone, interval_seconds,
                 timestamp_format, required_metrics, mapping):
    """Stage selected columns and measure the original UTC timestamp coverage."""
    try:
        zone = ZoneInfo(source_timezone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise DataQualityError(f"Unknown source time zone {source_timezone!r}. Install tzdata if needed.") from exc

    required_keys = ["timestamp", *required_metrics]
    with source_rows(path, sheet_name, required_keys, mapping) as (rows, indexes, chosen_sheet):
        timestamp_index = indexes["timestamp"]
        metric_indices = [indexes.get(key) for key in METRIC_KEYS]
        first_epoch = None
        previous_epoch = None
        pending = []
        seen_epochs = set()
        inferred_epochs = set()
        duplicates = 0
        unplaced_blank_rows = 0
        with csv_path.open("w", encoding="utf-8", newline="") as staged, ExitStack() as files:
            writer = csv.writer(staged)
            writer.writerow(["_epoch", "_source_row", *METRIC_KEYS])
            timed_rows = timed_source_rows(rows, timestamp_index, metric_indices, zone, timestamp_format)
            step_seconds = interval_seconds
            if step_seconds is None:
                buffer = files.enter_context(
                    TemporaryFile(mode="w+", encoding="utf-8", newline="", dir=csv_path.parent)
                )
                step_seconds, timed_rows = infer_timed_rows(timed_rows, buffer)

            for epoch, row_number, values in timed_rows:
                if epoch is None:
                    pending.append((row_number, values))
                    continue

                if epoch in seen_epochs:
                    writer.writerow([epoch, row_number, *values])
                    inferred_epochs.discard(epoch)
                    duplicates += 1
                    continue

                if previous_epoch is None:
                    first_epoch = epoch
                    unplaced_blank_rows += len(pending)
                else:
                    delta = epoch - previous_epoch
                    if delta < 0 or delta % step_seconds:
                        raise DataQualityError(
                            f"Source row {row_number}: unordered or off-grid timestamp "
                            f"{utc_label(epoch)} after {utc_label(previous_epoch)} "
                            f"with a {step_seconds}-second interval."
                        )
                    gap_slots = delta // step_seconds - 1
                    if len(pending) == gap_slots:
                        for offset, (pending_row, raw_values) in enumerate(pending, 1):
                            inferred_epoch = previous_epoch + offset * step_seconds
                            writer.writerow([inferred_epoch, pending_row, *raw_values])
                            seen_epochs.add(inferred_epoch)
                            inferred_epochs.add(inferred_epoch)
                    else:
                        unplaced_blank_rows += len(pending)
                pending.clear()
                writer.writerow([epoch, row_number, *values])
                seen_epochs.add(epoch)
                previous_epoch = epoch

        if first_epoch is None:
            raise DataQualityError("No timestamped data rows found.")
        expected = (previous_epoch - first_epoch) // step_seconds + 1
        actual = len(seen_epochs) - len(inferred_epochs)
        missing = expected - actual
        missing_pct = 100 * missing / expected
        if 100 * missing > MAX_MISSING_TIMESTAMP_PCT * expected:
            raise DataQualityError(
                f"Missing {missing:,} of {expected:,} expected timestamps at a {step_seconds}-second interval "
                f"({missing_pct:.4f}%; {actual:,} actual). Maximum: {MAX_MISSING_TIMESTAMP_PCT}%. "
                f"Range: {utc_label(first_epoch)} to {utc_label(previous_epoch)}."
            )
        coverage = {
            "interval_seconds": step_seconds,
            "worksheet": chosen_sheet,
            "expected_timestamp_slots": expected,
            "actual_timestamp_slots": actual,
            "missing_timestamp_slots": missing,
            "missing_timestamp_pct": missing_pct,
            "timestamp_slots_inserted": expected - len(seen_epochs),
            "blank_timestamps_reconstructed": len(inferred_epochs),
            "blank_timestamp_rows_unplaced": unplaced_blank_rows + len(pending),
            "duplicate_rows_removed": duplicates,
        }
        return first_epoch, previous_epoch, coverage


def finite(column):
    return column.isNotNull() & ~F.isnan(column) & (F.abs(column) < F.lit(float("inf")))


def numeric_value(column):
    """Parse number text without guessing a value for malformed strings."""
    text = F.trim(F.translate(column, "\u00a0\u202f\u2212", "  -"))
    # An isolated dot is a decimal point. Comma-only 3-digit groups use
    # thousands notation, e.g. 1,234. Other comma decimals, e.g. 12,5, are accepted.
    canonical = (
        F.when(text.rlike(r"^[+-]?\d{1,3}(,\d{3})+(\.\d+)?$"), F.regexp_replace(text, ",", ""))
        .when(
            text.rlike(r"^[+-]?\d{1,3}(\.\d{3})+,\d+$"),
            F.regexp_replace(F.regexp_replace(text, r"\.", ""), ",", "."),
        )
        .when(
            text.rlike(r"^[+-]?\d{1,3}( \d{3})+([.,]\d+)?$"),
            F.regexp_replace(F.regexp_replace(text, " ", ""), ",", "."),
        )
        .when(text.rlike(r"^[+-]?(\d+,\d+|,\d+)$"), F.regexp_replace(text, ",", "."))
        .when(text.rlike(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$"), text)
    )
    # The regex branches produce decimal strings or null, so this cast also works under ANSI mode.
    parsed = canonical.cast("double")
    return F.when(finite(parsed), parsed)


def with_neighbor_buckets(source, window_seconds):
    bucket_seconds = max(DAY_SECONDS, window_seconds)
    bucket = F.floor(F.col("_epoch") / bucket_seconds).cast("long")
    offset = F.col("_epoch") - bucket * bucket_seconds
    # Copy the edges into adjacent buckets so each record can see the full window.
    previous_bucket = F.when(offset < window_seconds, bucket - 1)
    next_bucket = F.when(offset >= bucket_seconds - window_seconds, bucket + 1)
    window_buckets = F.filter(
        F.array(bucket, previous_bucket, next_bucket), lambda value: value.isNotNull()
    )
    return source.withColumn("_bucket", bucket).withColumn("_window_bucket", F.explode(window_buckets))


def resolve_duplicate_demand(source, tolerance_pct, step_seconds, window_seconds):
    """Pick one source row per UTC interval and average its demand when readings agree."""
    grouped = source.groupBy("_epoch").agg(
        F.collect_list(F.struct("_source_row", *METRIC_KEYS)).alias("_candidates"),
        F.min("demand").alias("_min_demand"),
        F.max("demand").alias("_max_demand"),
        F.avg("demand").alias("_mean_demand"),
    )
    difference = F.col("_max_demand") - F.col("_min_demand")
    scale = F.greatest(F.abs("_min_demand"), F.abs("_max_demand"))
    agrees = (F.col("_min_demand") == F.col("_max_demand")) | (
        difference / F.when(scale > 0, scale) <= tolerance_pct / 100.0
    )
    mean_demand = F.when(finite(F.col("_mean_demand")), F.col("_mean_demand")).otherwise(F.col("_min_demand"))
    trusted = grouped.withColumn("_trusted", F.when(agrees, F.struct("_epoch", mean_demand.alias("value"))))
    window = Window.partitionBy("_window_bucket").orderBy("_epoch")
    nearby = with_neighbor_buckets(trusted, window_seconds).withColumns({
        "_before": F.last("_trusted", ignorenulls=True).over(
            window.rangeBetween(-window_seconds, -step_seconds)
        ),
    }).where(F.col("_bucket") == F.col("_window_bucket"))

    agreed = F.col("_trusted.value")
    target = F.coalesce(agreed, F.col("_before.value"))
    candidates = nearby.select(
        "_epoch", agreed.alias("_agreed_demand"), target.alias("_target"),
        F.explode("_candidates").alias("_reading"),
    )
    reading = F.col("_reading")
    choice = Window.partitionBy("_epoch").orderBy(
        reading["demand"].isNull(),
        F.abs(reading["demand"] - F.col("_target")).asc_nulls_last(),
        reading["_source_row"],
    )
    return candidates.withColumn("_rank", F.row_number().over(choice)).where(F.col("_rank") == 1).select(
        "_epoch", F.coalesce(F.col("_agreed_demand"), reading["demand"]).alias("demand"),
        *[reading[key].alias(key) for key in METRIC_KEYS if key != "demand"],
    )


def fill_from_history(grid, baseline_timezone):
    """Fill from earlier actual readings, using local calendar patterns for load and generation."""
    local = F.from_utc_timestamp(F.timestamp_seconds("_epoch"), baseline_timezone)
    calendar = grid.select(
        "*",
        F.datediff(F.to_date(local), F.lit("1970-01-01")).alias("_local_day"),
        F.dayofweek(local).alias("_weekday"),
        F.date_format(local, "HH:mm:ss").alias("_clock"),
    )
    prior_weeks = Window.partitionBy("_weekday", "_clock").orderBy("_local_day").rangeBetween(
        Window.unboundedPreceding, -1
    )
    prior_days = Window.partitionBy("_clock").orderBy("_local_day").rangeBetween(
        Window.unboundedPreceding, -1
    )
    prior_readings = Window.orderBy("_epoch").rowsBetween(Window.unboundedPreceding, -1)

    fills = []
    for key in METRIC_KEYS:
        actual = F.col(key)
        candidates = [actual]
        if key in ("demand", "solar_mwh", "wind_mwh"):
            reading = F.when(actual.isNotNull(), F.struct("_epoch", actual.alias("value")))
            # The epoch in the struct picks the latest actual, even on a repeated local hour.
            candidates.extend([
                F.max(reading).over(prior_weeks)["value"],
                F.max(reading).over(prior_days)["value"],
            ])
        candidates.append(F.last(actual, ignorenulls=True).over(prior_readings))
        fills.append(F.coalesce(*candidates).alias(key))

    return calendar.select(
        "_epoch", *fills,
        *[F.col(key).isNull().alias(f"_was_missing_{key}") for key in METRIC_KEYS],
    )


def fill_grid(spark, csv_path, first_epoch, last_epoch, coverage, duplicate_tolerance_pct, baseline_timezone):
    step_seconds = coverage["interval_seconds"]
    schema = T.StructType([
        T.StructField("_epoch", T.LongType()),
        T.StructField("_source_row", T.LongType()),
        *(T.StructField(key, T.StringType()) for key in METRIC_KEYS),
    ])
    source = (
        spark.read.option("header", True).option("multiLine", True).option("escape", '"')
        .schema(schema).csv(unquote(csv_path.as_uri()))
    )
    source = source.select(
        "_epoch", "_source_row", *[numeric_value(F.col(key)).alias(key) for key in METRIC_KEYS]
    )
    if coverage["duplicate_rows_removed"]:
        source = resolve_duplicate_demand(
            source, duplicate_tolerance_pct, step_seconds, max(HOUR_SECONDS, step_seconds)
        )
    expected = spark.range(first_epoch, last_epoch + 1, step_seconds).toDF("_epoch")
    grid = expected.join(source, "_epoch", "left")
    return fill_from_history(grid, baseline_timezone)


@contextmanager
def clean_station_export(
    spark, source_path, *, sheet_name=None, source_timezone="UTC", timestamp_format="auto",
    duplicate_demand_tolerance_pct=DEFAULT_DUPLICATE_TOLERANCE_PCT,
    interval_seconds=None, required_metrics=METRIC_KEYS, column_mapping=None,
):
    """Yield the complete UTC grid and cleaning diagnostics."""
    if not math.isfinite(duplicate_demand_tolerance_pct) or duplicate_demand_tolerance_pct < 0:
        raise DataQualityError("Duplicate demand tolerance must be a finite, non-negative percentage.")
    if interval_seconds is not None and (type(interval_seconds) is not int or interval_seconds < 1):
        raise DataQualityError("The interval must be a positive whole number of seconds.")

    # Give this pipeline UTC formatting without changing the caller's session.
    spark = spark.newSession()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    with TemporaryDirectory(prefix="spark_50hertz_") as directory:
        csv_path = Path(directory) / "staged.csv"
        first, last, coverage = stage_source(
            source_path, csv_path, sheet_name, source_timezone, interval_seconds,
            timestamp_format, required_metrics, column_mapping or {},
        )
        filled = fill_grid(
            spark, csv_path, first, last, coverage, duplicate_demand_tolerance_pct, source_timezone
        ).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            summary = filled.agg(
                F.count("*").alias("rows"),
                *[
                    F.count(F.when(F.col(f"_was_missing_{key}") & F.col(key).isNotNull(), 1))
                    .alias(f"_filled_{key}")
                    for key in METRIC_KEYS
                ],
                *[
                    F.count(F.when(F.col(key).isNull(), 1)).alias(f"_unfilled_{key}")
                    for key in METRIC_KEYS
                ],
            ).first()
            result = filled.orderBy("_epoch").select(
                F.date_format(F.timestamp_seconds("_epoch"), OUTPUT_FORMAT).alias("timestamp"),
                *[F.col(key).alias(output) for _, key, output in METRICS],
            )
            report = {
                "rows": summary.rows,
                **coverage,
                "cells_imputed": {output: summary[f"_filled_{key}"] for _, key, output in METRICS},
                "cells_unfilled_no_history": {output: summary[f"_unfilled_{key}"] for _, key, output in METRICS},
                "first_utc": utc_label(first),
                "last_utc": utc_label(last),
            }
            yield result, report
        finally:
            filled.unpersist()


def save_inspection_csv(dataframe, destination):
    """Stream the ordered Spark result into one CSV and replace it only after success."""
    with TemporaryDirectory(dir=destination.parent, prefix=".cleaned-csv-") as directory:
        temporary_path = Path(directory) / "data.csv"
        with temporary_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(dataframe.columns)
            writer.writerows(dataframe.toLocalIterator())
        temporary_path.replace(destination)


def parse_interval(value):
    match = re.fullmatch(r"([1-9][0-9]*)([smhd]?)", value.strip().lower())
    if not match:
        raise argparse.ArgumentTypeError("Use a positive interval such as 30s, 5m, 1h, or 1d.")
    amount, unit = match.groups()
    return int(amount) * {"s": 1, "": 60, "m": 60, "h": HOUR_SECONDS, "d": DAY_SECONDS}[unit]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--sheet")
    parser.add_argument("--source-timezone", default="UTC")
    parser.add_argument("--timestamp-format", default="auto")
    parser.add_argument("--metrics", nargs="+", choices=METRIC_KEYS, default=METRIC_KEYS)
    parser.add_argument("--column-mapping", type=json.loads, default={})
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--interval", type=parse_interval, metavar="DURATION",
        help="Fixed interval such as 30s, 5m, 1h, or 1d. Defaults to automatic detection.",
    )
    parser.add_argument(
        "--duplicate-demand-tolerance-pct", type=float, default=DEFAULT_DUPLICATE_TOLERANCE_PCT,
        help="Average duplicate demand when the spread is within this percentage (default: 1).",
    )
    args = parser.parse_args()

    if os.name == "nt":
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    spark = (
        SparkSession.builder.master("local[2]").appName("StationImport")
        .config("spark.driver.memory", "640m")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        with clean_station_export(
            spark, args.source, sheet_name=args.sheet, source_timezone=args.source_timezone,
            timestamp_format=args.timestamp_format, column_mapping=args.column_mapping,
            required_metrics=args.metrics, duplicate_demand_tolerance_pct=args.duplicate_demand_tolerance_pct,
            interval_seconds=args.interval,
        ) as (dataframe, report):
            save_inspection_csv(dataframe, args.output.resolve())
            args.report.write_text(json.dumps(report), encoding="utf-8")
    except (DataQualityError, UnicodeError, csv.Error, BadZipFile, InvalidFileException) as exc:
        parser.exit(2, f"Data rejected: {exc}\n")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
