"""Week 3, Task 5: evaluate the production readiness of the platform.

Prerequisites: the initial load (data_ingestion.py) and the update files (simulate_new_data.py).
Everything runs in one Spark session, in this order:

1. Overhead trials. The same update is loaded under three configurations -- everything on,
   PLATFORM_VALIDATION=off, PLATFORM_MONITORING=off -- `--reps` times each, in rotating order,
   after one discarded warm-up. After every trial the curated and quarantine tables are put
   back to their pre-update Delta versions with RESTORE, so every trial starts from the same
   state. Validation overhead = baseline - no_validation, monitoring overhead = baseline -
   no_monitoring (medians of wall-clock time around each dataset's execute_pipeline).
2. Storage before the real update.
3. The real update: products rebuilt in full (the Week 2 way, timed), then the update loaded
   with everything on, then the refresh planner (incremental), a second planner run with
   nothing new (no-op), a from-scratch verification, and a forced full refresh for comparison.
4. Storage after the real update; the difference is what the update and the Week 3
   components added.

Raw measurements go to evaluation_results_week_3.json; the report is
"Evaluation Report Week 3.md". Takes about 12 minutes with the default 3 repetitions.
"""
import argparse
import glob
import json
import os
import shutil
import statistics
import time
from contextlib import contextmanager
from datetime import datetime

import data_products  # noqa: F401  (sets PYSPARK_SUBMIT_ARGS before pyspark is imported)
import continuous_data_insert
import monitoring
import refresh_manager
import validation
from data_ingestion import build_spark
from data_products import PRODUCTS_DIR, delta_sql_name
from pyspark.sql import SparkSession

RESULTS_FILE = "evaluation_results_week_3.json"

CURATED_TABLES = {
    "weather": "output_data/weather",
    "air_quality": "output_data/air_quality",
    "taxi_zones": "output_data/taxi_zones",
    "trip_data": "output_data/trip_data",
    "integrated_taxi_trips": "output_data/integrated_taxi_trips",
}
# the tables a trial writes to (taxi zones get no update)
TRIAL_TABLES = ["weather", "air_quality", "trip_data", "integrated_taxi_trips"]

CONFIGS = {
    "baseline": {},
    "no_validation": {"PLATFORM_VALIDATION": "off"},
    "no_monitoring": {"PLATFORM_MONITORING": "off"},
}


@contextmanager
def environment(overrides: dict):
    saved = {k: os.environ.get(k) for k in overrides}
    os.environ.update(overrides)
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


# --- restoring the pre-update state -------------------------------------------------

def delta_version(spark: SparkSession, path: str):
    if not os.path.isdir(os.path.join(path, "_delta_log")):
        return None
    return int(spark.sql(f"DESCRIBE HISTORY {delta_sql_name(path)} LIMIT 1").first()["version"])


def quarantine_tables() -> list:
    return sorted(p for p in glob.glob(f"{validation.QUARANTINE_DIR}/*") if os.path.isdir(p))


def snapshot(spark: SparkSession) -> dict:
    paths = [CURATED_TABLES[t] for t in TRIAL_TABLES] + quarantine_tables()
    return {path: delta_version(spark, path) for path in paths}


def restore(spark: SparkSession, state: dict) -> None:
    """Put every table a trial wrote to back to its snapshot version; drop tables the trial created."""
    for path, version in state.items():
        if version is not None and delta_version(spark, path) != version:
            spark.sql(f"RESTORE TABLE {delta_sql_name(path)} TO VERSION AS OF {version}")
    for path in quarantine_tables():
        if path not in state:
            shutil.rmtree(path)
    spark.catalog.clearCache()
    spark.sparkContext._jvm.System.gc()  # let the context cleaner drop the last trial's checkpoints


# --- one load of the update ---------------------------------------------------------------

def run_update(spark: SparkSession, label: str, overrides: dict) -> dict:
    monitoring.start_run(label)
    with environment(overrides):
        start = time.time()
        timings = continuous_data_insert.load_updates(spark)
        total = time.time() - start
    return {
        "total_seconds": round(total, 2),
        "datasets": {name: {k: t.get(k) for k in ("wall_seconds", "execution_time_seconds", "validate_seconds",
                                                    "write_seconds", "processed_records", "inserted_records",
                                                    "rejected_records", "duplicates_skipped", "status")}
                     for name, t in timings.items()},
    }


def summarize(trials: list) -> dict:
    totals = [t["total_seconds"] for t in trials]
    datasets = {}
    for name in trials[0]["datasets"]:
        walls = [t["datasets"][name]["wall_seconds"] for t in trials]
        datasets[name] = {"median_seconds": round(statistics.median(walls), 2),
                          "min_seconds": min(walls), "max_seconds": max(walls)}
    return {"median_seconds": round(statistics.median(totals), 2), "min_seconds": min(totals),
            "max_seconds": max(totals), "runs_seconds": totals, "datasets": datasets}


def overhead(configs: dict, without: str) -> dict:
    base, other = configs["baseline"], configs[without]
    result = {"total": {
        "seconds": round(base["median_seconds"] - other["median_seconds"], 2),
        "pct_of_run_without": round(100 * (base["median_seconds"] - other["median_seconds"])
                                    / other["median_seconds"], 1)}}
    for name, stats in base["datasets"].items():
        without_seconds = other["datasets"][name]["median_seconds"]
        result[name] = {"seconds": round(stats["median_seconds"] - without_seconds, 2),
                        "pct_of_run_without": round(100 * (stats["median_seconds"] - without_seconds)
                                                    / without_seconds, 1) if without_seconds else None}
    return result


def overhead_trials(spark: SparkSession, reps: int) -> dict:
    state = snapshot(spark)
    print(f"\n=== overhead trials: {reps} x {list(CONFIGS)}, restoring to {state} ===")
    run_update(spark, "evaluation:warmup", CONFIGS["baseline"])  # JIT, jar and OS-cache warm-up
    restore(spark, state)

    order = list(CONFIGS)
    trials = {name: [] for name in CONFIGS}
    for rep in range(reps):
        for name in order[rep % len(order):] + order[:rep % len(order)]:
            print(f"\n--- trial {rep + 1}/{reps}: {name} ---")
            trials[name].append(run_update(spark, f"evaluation:{name}", CONFIGS[name]))
            restore(spark, state)
            print(f"--- {name}: {trials[name][-1]['total_seconds']:.1f}s ---")

    configs = {name: summarize(runs) for name, runs in trials.items()}
    baseline_runs = trials["baseline"]
    internal_validate = [sum(d["validate_seconds"] or 0 for d in t["datasets"].values()) for t in baseline_runs]
    return {
        "configs": configs,
        "validation_overhead": overhead(configs, "no_validation"),
        "monitoring_overhead": overhead(configs, "no_monitoring"),
        "validate_seconds_reported_by_pipeline": round(statistics.median(internal_validate), 2),
        "trials": trials,
    }


# --- storage ------------------------------------------------------------------------------------

def disk_bytes(path: str, only: str = None) -> int:
    total = 0
    for root, _, files in os.walk(path):
        if only and only not in root:
            continue
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
    return total


def table_storage(spark: SparkSession, path: str) -> dict:
    detail = spark.sql(f"DESCRIBE DETAIL {delta_sql_name(path)}").first()
    return {"live_bytes": int(detail["sizeInBytes"]), "live_files": int(detail["numFiles"]),
            "disk_bytes": disk_bytes(path), "log_bytes": disk_bytes(path, "_delta_log"),
            "version": delta_version(spark, path)}


def measure_storage(spark: SparkSession) -> dict:
    groups = {
        "curated": dict(CURATED_TABLES),
        "quarantine": {os.path.basename(p): p for p in quarantine_tables()},
        "monitoring": {name: monitoring.table_path(name)
                       for name in (monitoring.PIPELINE_RUNS, monitoring.VALIDATION_RESULTS)},
        "data_products": {os.path.basename(p): p for p in sorted(glob.glob(f"{PRODUCTS_DIR}/*"))},
    }
    storage = {}
    for group, tables in groups.items():
        storage[group] = {name: table_storage(spark, path) for name, path in tables.items()
                          if os.path.isdir(os.path.join(path, "_delta_log"))}
        storage[group]["_total"] = {k: sum(t[k] for t in storage[group].values())
                                    for k in ("live_bytes", "live_files", "disk_bytes", "log_bytes")}
    return storage


# --- the real update and the refresh -----------------------------------------------------------

def real_update(spark: SparkSession) -> dict:
    result = {}
    print("\n=== products rebuilt in full before the update (Week 2 behaviour) ===")
    refresh_manager.register_views(spark)
    result["full_refresh_before_update"] = refresh_manager.execute_plan(
        spark, refresh_manager.build_plan(spark, force_full=True))

    print("\n=== the incremental update ===")
    result["update"] = run_update(spark, "incremental_update", {})

    print("\n=== refresh: only what the update affected ===")
    start = time.time()
    summary = refresh_manager.refresh_affected(spark)
    summary["seconds_including_planning"] = round(time.time() - start, 2)
    result["incremental_refresh"] = summary

    print("\n=== refresh again with nothing new (no-op) ===")
    start = time.time()
    summary = refresh_manager.refresh_affected(spark)
    summary["seconds_including_planning"] = round(time.time() - start, 2)
    result["noop_refresh"] = summary

    print("\n=== verification: incremental result vs. a from-scratch build ===")
    result["verification"] = refresh_manager.verify(spark)

    print("\n=== forced full refresh of the same data, for comparison ===")
    start = time.time()
    summary = refresh_manager.refresh_affected(spark, force_full=True)
    summary["seconds_including_planning"] = round(time.time() - start, 2)
    result["full_refresh_after_update"] = summary
    return result


def initial_load_seconds(spark: SparkSession) -> dict:
    """What loading the initial release took, from the monitoring table: the cost of a rebuild."""
    monitoring.register_views(spark)
    rows = spark.sql("""
        SELECT dataset, execution_time_seconds, processed_records
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dataset ORDER BY started_at) AS rn
              FROM pipeline_runs WHERE pipeline = 'initial_load')
        WHERE rn = 1
    """).collect()
    return {r["dataset"]: {"execution_time_seconds": r["execution_time_seconds"],
                           "processed_records": r["processed_records"]} for r in rows}


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate the platform (Week 3, Task 5).")
    parser.add_argument("--reps", type=int, default=3, help="trials per configuration (default 3)")
    parser.add_argument("--skip-trials", action="store_true", help="only the real update and the refresh")
    args = parser.parse_args()

    for path in (continuous_data_insert.WEATHER_PATH, continuous_data_insert.AIR_QUALITY_PATH,
                 continuous_data_insert.TRIP_DATA_PATH):
        if not os.path.exists(path):
            raise SystemExit(f"{path} is missing: run simulate_new_data.py first")
    if not os.path.isdir(os.path.join(CURATED_TABLES["integrated_taxi_trips"], "_delta_log")):
        raise SystemExit("no integrated table: run data_ingestion.py first")

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    results = {"started_at": datetime.now().isoformat(timespec="seconds"), "reps": args.reps}
    results["initial_load"] = initial_load_seconds(spark)

    if not args.skip_trials:
        results["overhead_trials"] = overhead_trials(spark, args.reps)

    results["storage_before_update"] = measure_storage(spark)
    results.update(real_update(spark))
    results["storage_after_update"] = measure_storage(spark)
    results["finished_at"] = datetime.now().isoformat(timespec="seconds")

    with open(RESULTS_FILE, "w") as handle:
        json.dump(results, handle, indent=2, default=str)
    print(f"\nraw measurements written to {RESULTS_FILE}")
    spark.stop()


if __name__ == "__main__":
    main()
