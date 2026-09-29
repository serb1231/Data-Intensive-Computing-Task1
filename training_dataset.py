"""Week 4, Task 1: generate the training dataset for hourly taxi demand from the platform's Delta tables.

One row per (pickup zone, local hour) of the window:

  target           trip_count: the trips that started in the zone during the hour, 0 when none did.
                   Trips quarantined only for their passenger count are counted too (ml_config)
  zone features    pulocationid, borough and service zone, from the taxi_zones table
  hourly features  the weather and air quality of the hour, from the integrated table
  lag features     the zone's trip_count 1 hour, 1 day and 1 week earlier on the local clock
  split            train, validation or test: consecutive whole weeks, in that order

Columns are kept as the platform stores them. Deriving calendar features, encoding, imputing and
scaling belong to the feature pipeline (Task 2), which is fitted on the train split only.

`python training_dataset.py` builds the dataset declared in ml_config.py from pinned versions of
the integrated, taxi_zones and trip quarantine tables, writes it to output_data/ml/<name>
partitioned by split, and records what it built (source versions, window, split sizes, null
shares, signal per feature) in the Delta commit and in training_dataset_week_4.json.
"""
import argparse
import json
import time
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta

from data_products import delta_sql_name, discover_window, format_months, partition_predicate
from analytical_queries import INTEGRATED_PATH, TAXI_ZONES_PATH
import ml_config
from ml_config import TrainingDatasetConfig
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from validation import quarantine_path

SPLITS = ["train", "validation", "test"]
KEYS = ["pickup_hour", "pulocationid"]


# --- time --------------------------------------------------------------------------------

def local_hour(column: str) -> Column:
    """The start of the local hour a timestamp_ntz falls in.

    date_trunc, and with it the integrated table's pickup_hour, converts through the Spark session
    time zone. Where that zone has a daylight-saving gap and New York does not (2024-03-31 02:00 in
    Europe/Stockholm), the trips of one hour land in the next. Building the hour from the fields of
    the wall-clock time does not depend on the session time zone.
    """
    ts = F.col(column)
    return F.make_timestamp_ntz(F.year(ts), F.month(ts), F.dayofmonth(ts), F.hour(ts), F.lit(0), F.lit(0))


def ntz(moment) -> Column:
    """A date or datetime as a timestamp_ntz literal; F.lit(datetime) would be a session-zone timestamp."""
    return F.lit(moment.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp_ntz")


def in_period(column: str, start, end) -> Column:
    return (F.col(column) >= ntz(start)) & (F.col(column) < ntz(end))


def next_month(year: int, month: int) -> date:
    return date(year + month // 12, month % 12 + 1, 1)


# --- window and splits -------------------------------------------------------------------

def resolve_window(spark: SparkSession, integrated: DataFrame, config: TrainingDatasetConfig) -> tuple:
    """(start, end, months): the configured [start, end), clipped to the months the platform reports on.

    discover_window leaves out months with too few trips to be real, such as the mis-dated trips in
    2009-01 and 2023-12, so they neither start the window nor count as lag history.
    """
    months = discover_window(spark, integrated)
    if not months:
        raise SystemExit(f"{INTEGRATED_PATH} has no month with enough trips to build a dataset from")

    first = date(*months[0], 1)
    start = max(first, date.fromisoformat(config.window_start)) if config.window_start else first
    if config.window_end:
        end = min(next_month(*months[-1]), date.fromisoformat(config.window_end))
    else:
        last_pickup = integrated.where(partition_predicate(months)).agg(F.max("tpep_pickup_datetime")).first()[0]
        end = last_pickup.date() + timedelta(days=1)
    return start, end, months


def split_boundaries(config: TrainingDatasetConfig, start: date, end: date) -> tuple:
    """(validation_start, test_start): whole weeks counted back from the end of the window.

    Each held-out split then contains every weekday equally often, and it is later than everything
    it is evaluated against. Train is whatever comes before, and has to be at least a week.
    """
    test_start = end - timedelta(weeks=config.test_weeks)
    validation_start = test_start - timedelta(weeks=config.validation_weeks)
    if validation_start - start < timedelta(weeks=1):
        raise ValueError(f"the window {start} to {end} leaves less than a week to train on")
    return validation_start, test_start


# --- the dataset -------------------------------------------------------------------------

def quarantined_pickups(quarantine: DataFrame, reasons: tuple) -> DataFrame:
    """The trips quarantined for these rules and no other, once each.

    A trip can be in the quarantine twice: a release that re-delivers it, or a re-run, appends it again.
    """
    other_reasons = F.array_except("_rejection_reasons", F.array(*[F.lit(reason) for reason in reasons]))
    return (quarantine.where(F.size(other_reasons) == 0)
            .dropDuplicates(["surrogate_key"])
            .select("tpep_pickup_datetime", "pulocationid"))


def build_training_dataset(trips: DataFrame, zones: DataFrame, config: TrainingDatasetConfig,
                           start: date, end: date, quarantined: DataFrame = None) -> DataFrame:
    """One row per selected zone and local hour of [start, end), with its split.

    trips is the integrated table and zones the taxi_zones table; quarantined holds more pickups
    (tpep_pickup_datetime, pulocationid) to count, see quarantined_pickups. Trips before start are read
    as far back as the longest lag, so the first hours of the window get lags when the platform has them.
    """
    target, hourly = config.target, config.hourly_columns
    missing = [c for c in hourly if c not in trips.columns]
    if missing:
        raise ValueError(f"hourly feature(s) {missing} are not in the integrated table")

    validation_start, test_start = split_boundaries(config, start, end)
    history_start = datetime(start.year, start.month, start.day) - timedelta(hours=max(config.lag_hours, default=0))

    pickups = trips.select("tpep_pickup_datetime", "pulocationid", *hourly)
    if quarantined is not None:
        # a quarantined trip carries no weather; max() below skips its NULLs, so the hour keeps its own
        pickups = pickups.unionByName(quarantined, allowMissingColumns=True)

    # one pass over the pickups: how many per (zone, hour), with the weather and air quality of the hour
    per_zone_hour = (pickups
                     .where(in_period("tpep_pickup_datetime", history_start, end))
                     .withColumn("pickup_hour", local_hour("tpep_pickup_datetime"))
                     .groupBy(*KEYS)
                     .agg(F.count("*").alias(target), *[F.max(c).alias(c) for c in hourly])
                     .cache())

    # the platform joined weather and air quality on the pickup hour, so they are the same for every
    # trip of an hour. An hour without a single trip in the city (the DST gap) has no row.
    hours = per_zone_hour.groupBy("pickup_hour").agg(*[F.max(c).alias(c) for c in hourly])

    # zones: places with regular pickups, judged on the training weeks only, so the weeks held out
    # for evaluation have no say in which zones the dataset contains
    train_days = (validation_start - start).days
    train_trips = (per_zone_hour.where(in_period("pickup_hour", start, validation_start))
                   .groupBy("pulocationid").agg(F.sum(target).alias("train_trips")))
    selected = (zones.where(config.zone_filter)
                .join(train_trips, zones["locationid"] == train_trips["pulocationid"])
                .where(F.col("train_trips") >= config.min_zone_trips_per_day * train_days)
                .select("pulocationid", *[F.col(src).alias(dst) for src, dst in config.zone_features.items()]))

    # every selected zone in every hour: an hour without a pickup is a demand of 0, not a missing row
    series = (selected.select("pulocationid").crossJoin(hours.select("pickup_hour"))
              .join(per_zone_hour.select(*KEYS, target), KEYS, "left")
              .fillna(0, subset=[target]))

    # lags join the zero-filled series on the local clock: NULL only where the platform has no such
    # hour (before the first month, or the hour skipped when clocks go forward)
    dataset = series.where(in_period("pickup_hour", start, end))
    for lag, column in zip(config.lag_hours, config.lag_columns):
        earlier = series.select("pulocationid",
                                (F.col("pickup_hour") + F.expr(f"INTERVAL {lag} HOURS")).alias("pickup_hour"),
                                F.col(target).alias(column))
        dataset = dataset.join(earlier, KEYS, "left")

    split = (F.when(F.col("pickup_hour") < ntz(validation_start), "train")
             .when(F.col("pickup_hour") < ntz(test_start), "validation")
             .otherwise("test"))
    return (dataset.join(selected, "pulocationid").join(hours, "pickup_hour")
            .withColumn("split", split)
            .select(*KEYS, target, *config.zone_features.values(), *hourly, *config.lag_columns, "split"))


def write_dataset(dataset: DataFrame, config: TrainingDatasetConfig, metadata: dict) -> int:
    """Replace the dataset's Delta table, one file per split; returns the version written."""
    (dataset.repartition("split")
     .write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true")
     .option("userMetadata", json.dumps(metadata, default=str))
     .partitionBy("split")
     .save(config.path))
    return latest_version(dataset.sparkSession, config.path)


# --- what was built ----------------------------------------------------------------------

def describe_splits(dataset: DataFrame, config: TrainingDatasetConfig) -> dict:
    """Per split: size, time span, target distribution and the share of NULLs of every feature."""
    target, features = config.target, config.feature_columns
    rows = dataset.groupBy("split").agg(
        F.count("*").alias("rows"),
        F.countDistinct("pulocationid").alias("zones"),
        F.countDistinct("pickup_hour").alias("hours"),
        F.min("pickup_hour").alias("first_hour"),
        F.max("pickup_hour").alias("last_hour"),
        F.sum(target).alias("trips"),
        F.round(F.avg(target), 3).alias("target_mean"),
        F.round(F.stddev(target), 3).alias("target_stddev"),
        F.max(target).alias("target_max"),
        F.round(F.avg((F.col(target) == 0).cast("double")), 4).alias("target_zero_share"),
        *[F.round(F.avg(F.col(c).isNull().cast("double")), 4).alias(f"null:{c}") for c in features],
    ).collect()

    splits = {}
    for row in rows:
        values = row.asDict()
        stats = {k: (str(v) if isinstance(v, datetime) else v)
                 for k, v in values.items() if k != "split" and not k.startswith("null:")}
        stats["null_share"] = {c: values[f"null:{c}"] for c in features if values[f"null:{c}"]}
        splits[values["split"]] = stats
    return {name: splits[name] for name in SPLITS if name in splits}


def eta_squared(df: DataFrame, value: str, groups: list) -> float:
    """The share of the variance of `value` explained by its mean per group (between / total sum of squares)."""
    mean = df.agg(F.avg(value)).first()[0]
    total = df.agg(F.sum((F.col(value) - mean) ** 2)).first()[0]
    between = (df.groupBy(*groups).agg(F.count("*").alias("n"), F.avg(value).alias("group_mean"))
               .agg(F.sum(F.col("n") * (F.col("group_mean") - mean) ** 2)).first()[0])
    return round(between / total, 4)


def r_squared(df: DataFrame, actual: str, predicted: str) -> float:
    mean = df.agg(F.avg(actual)).first()[0]
    row = df.agg(F.sum((F.col(actual) - F.col(predicted)) ** 2).alias("error"),
                 F.sum((F.col(actual) - mean) ** 2).alias("total")).first()
    return round(1 - row["error"] / row["total"], 4)


def feature_signal(dataset: DataFrame, config: TrainingDatasetConfig) -> dict:
    """How much each source tells about demand. Descriptive: fitted on the train split only.

    structure: the share of the target's variance explained by the mean per zone, per hour of the
      week, and per zone and hour of the week (eta squared, in sample). Location and calendar alone.
      The last one is also scored on the validation weeks, because with ~9 training rows per zone and
      hour of the week the in-sample figure flatters it.
    correlation: Pearson r of each quantity with the target, and with the residual left after the
      zone x hour-of-week mean. The residual column is what weather, air quality and the lags add on
      top of location and calendar. Codes get eta squared of the residual instead.
    """
    target = config.target
    with_hour_of_week = dataset.withColumn(
        "hour_of_week", (F.dayofweek("pickup_hour") - 1) * 24 + F.hour("pickup_hour"))
    train = with_hour_of_week.where("split = 'train'").cache()
    try:
        expected = train.groupBy("pulocationid", "hour_of_week").agg(F.avg(target).alias("expected"))
        validation = with_hour_of_week.where("split = 'validation'").join(
            expected, ["pulocationid", "hour_of_week"])
        structure = {
            "zone": eta_squared(train, target, ["pulocationid"]),
            "borough": eta_squared(train, target, ["pickup_borough"]),
            "hour_of_week": eta_squared(train, target, ["hour_of_week"]),
            "zone_x_hour_of_week": eta_squared(train, target, ["pulocationid", "hour_of_week"]),
            "zone_x_hour_of_week_r2_on_validation": r_squared(validation, target, "expected"),
        }

        residual = (train.join(expected, ["pulocationid", "hour_of_week"])
                    .withColumn("residual", F.col(target) - F.col("expected")))
        candidates = [*config.hourly_columns, *config.lag_columns]
        quantities = [c for c in candidates if c not in config.categorical_features]
        r = residual.agg(*[F.corr(target, c).alias(f"target:{c}") for c in quantities],
                         *[F.corr("residual", c).alias(f"residual:{c}") for c in quantities]).first()
        correlation = {c: {"r_target": round(r[f"target:{c}"], 4), "r_residual": round(r[f"residual:{c}"], 4)}
                       for c in quantities}
        for code in [c for c in candidates if c in config.categorical_features]:
            correlation[code] = {"eta_squared_residual": eta_squared(residual, "residual", [code])}
        return {"structure": structure, "correlation": correlation}
    finally:
        train.unpersist()


def column_dictionary(config: TrainingDatasetConfig) -> dict:
    """Every column of the dataset: its role, where it comes from and whether it is a code or a quantity."""
    kind = lambda c: "categorical" if c in config.categorical_features else "numeric"
    columns = {
        "pickup_hour": {"role": "key", "source": "integrated_taxi_trips.tpep_pickup_datetime, local hour",
                        "kind": "timestamp"},
        "pulocationid": {"role": "key, feature", "source": "taxi_zones.locationid", "kind": kind("pulocationid")},
        config.target: {"role": "target", "kind": "count",
                        "source": "integrated_taxi_trips" + (", quarantine/trip_data" if config.count_quarantined_for else "")
                                  + ": pickups per zone and hour"},
    }
    for source, column in config.zone_features.items():
        columns[column] = {"role": "feature", "source": f"taxi_zones.{source}", "kind": kind(column)}
    for dataset, hourly in config.hourly_features.items():
        for column in hourly:
            columns[column] = {"role": "feature", "source": f"integrated_taxi_trips.{column} ({dataset})",
                               "kind": kind(column)}
    for lag, column in zip(config.lag_hours, config.lag_columns):
        columns[column] = {"role": "feature", "source": f"{config.target}, {lag} h earlier", "kind": "count"}
    columns["split"] = {"role": "split", "source": "pickup_hour", "kind": "categorical"}
    return columns


# --- sources -----------------------------------------------------------------------------

def latest_version(spark: SparkSession, path: str) -> int:
    return int(spark.sql(f"DESCRIBE HISTORY {delta_sql_name(path)} LIMIT 1").first()["version"])


def version_at(spark: SparkSession, path: str, moment: datetime) -> int:
    """The version a Delta table had at that moment: its last commit up to then."""
    history = spark.sql(f"DESCRIBE HISTORY {delta_sql_name(path)}")
    return int(history.where(F.col("timestamp") <= F.lit(moment)).agg(F.max("version")).first()[0])


def read_version(spark: SparkSession, path: str, version: int) -> DataFrame:
    return spark.read.format("delta").option("versionAsOf", version).load(path)


def pin_sources(spark: SparkSession, config: TrainingDatasetConfig, integrated_version: int = None) -> dict:
    """name -> (path, version) of every table the dataset is read from.

    The integrated table is read at the requested version (default: the current one), and the other
    tables as they were when that version was committed, so an older dataset can be rebuilt exactly.
    """
    paths = {"integrated_taxi_trips": INTEGRATED_PATH, "taxi_zones": TAXI_ZONES_PATH}
    if config.count_quarantined_for:
        paths["quarantine/trip_data"] = quarantine_path("Trip Data")
    if integrated_version is None:
        return {name: (path, latest_version(spark, path)) for name, path in paths.items()}

    committed = spark.sql(f"DESCRIBE HISTORY {delta_sql_name(INTEGRATED_PATH)}") \
        .where(f"version = {integrated_version}").first()["timestamp"]
    return {name: (path, integrated_version if path == INTEGRATED_PATH else version_at(spark, path, committed))
            for name, path in paths.items()}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the Week 4 training dataset (Task 1) declared in "
                                                 "ml_config.py from the platform's Delta tables.")
    parser.add_argument("--start", metavar="YYYY-MM-DD", help="first day of the window (default: ml_config)")
    parser.add_argument("--end", metavar="YYYY-MM-DD", help="day after the last day of the window (default: ml_config)")
    parser.add_argument("--source-version", type=int, metavar="N",
                        help="build from this Delta version of the integrated table (default: the current one)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = replace(ml_config.TRAINING_DATASET,
                     window_start=args.start or ml_config.TRAINING_DATASET.window_start,
                     window_end=args.end or ml_config.TRAINING_DATASET.window_end)

    from data_ingestion import build_spark
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    started = time.time()

    # the versions read are pinned, so the manifest names exactly what the dataset was built from
    pinned = pin_sources(spark, config, args.source_version)
    tables = {name: read_version(spark, path, version) for name, (path, version) in pinned.items()}
    sources = {name: {"path": path, "version": version} for name, (path, version) in pinned.items()}

    start, end, months = resolve_window(spark, tables["integrated_taxi_trips"], config)
    validation_start, test_start = split_boundaries(config, start, end)
    boundaries = {"train": (start, validation_start), "validation": (validation_start, test_start),
                  "test": (test_start, end)}
    print("sources: " + ", ".join(f"{name} v{source['version']}" for name, source in sources.items())
          + f"; window {start} to {end}; " + ", ".join(f"{s} from {a}" for s, (a, _) in boundaries.items()))

    # the months discover_window found: mis-dated trips stay out of the window and out of the lags
    trips = tables["integrated_taxi_trips"].where(partition_predicate(months))
    quarantined = None
    if config.count_quarantined_for:
        quarantined = quarantined_pickups(tables["quarantine/trip_data"].where(partition_predicate(months)),
                                          config.count_quarantined_for)
    dataset = build_training_dataset(trips, tables["taxi_zones"], config, start, end, quarantined)
    dataset_version = write_dataset(dataset, config, {
        "dataset": config.name, "sources": sources, "window": [start, end],
        "splits": boundaries, "config": asdict(config)})
    build_seconds = round(time.time() - started, 2)

    written = spark.read.format("delta").load(config.path)
    splits = describe_splits(written, config)
    for name, (first_day, after_last_day) in boundaries.items():
        splits[name] = {"start": str(first_day), "end": str(after_last_day),
                        "weeks": round((after_last_day - first_day).days / 7, 2), **splits[name]}

    count_in_window = lambda df: df.where(in_period("tpep_pickup_datetime", start, end)).count()
    validated = count_in_window(trips)
    recovered = count_in_window(quarantined) if quarantined is not None else 0
    pickups = trips.select("tpep_pickup_datetime", "pulocationid")
    if quarantined is not None:
        pickups = pickups.unionByName(quarantined)
    zones_with_pickups = pickups.where(in_period("tpep_pickup_datetime", start, end)) \
        .select("pulocationid").distinct().count()
    in_dataset = sum(s["trips"] for s in splits.values())
    rows = sum(s["rows"] for s in splits.values())
    hours = sum(s["hours"] for s in splits.values())

    manifest = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "build_seconds": build_seconds,
        "dataset": {"name": config.name, "path": config.path, "delta_version": dataset_version, "rows": rows},
        "sources": sources,
        "window": {
            "start": str(start), "end": str(end),
            "months": format_months([m for m in months if date(*m, 1) < end and next_month(*m) > start]),
            "hours_expected": (end - start).days * 24,
            "hours_with_trips": hours,
        },
        "demand": {
            "validated_trips": validated,
            "quarantined_pickups_counted": recovered,
            "pickups": validated + recovered,
            "pickups_in_selected_zones": in_dataset,
            "coverage": round(in_dataset / (validated + recovered), 5),
            "zones_with_pickups": zones_with_pickups,
            "zones_selected": splits["train"]["zones"],
        },
        "splits": splits,
        "columns": column_dictionary(config),
        "signal": feature_signal(written, config),
        "config": asdict(config),
    }
    with open(ml_config.MANIFEST_PATH, "w") as handle:
        json.dump(manifest, handle, indent=2, default=str)

    print(json.dumps({k: manifest[k] for k in ("dataset", "window", "demand", "splits", "signal")},
                     indent=2, default=str))
    written.orderBy("pickup_hour", "pulocationid").show(5, truncate=False)
    print(f"wrote {config.path} (Delta version {dataset_version}) in {build_seconds} s; "
          f"manifest in {ml_config.MANIFEST_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
