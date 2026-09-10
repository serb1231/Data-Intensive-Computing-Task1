import glob
import json
import os
import shutil
import statistics
import time

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-memory 4g "
    "--packages io.delta:delta-spark_2.12:3.1.0 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, count, to_date, unix_timestamp, dayofmonth

SOURCE_TRIPS = "output_data/trip_data"
SOURCE_ZONES = "output_data/taxi_zones"
BENCH_ROOT = "benchmark_output"
RESULTS_FILE = "benchmark_results.json"

# the first execution pays JIT and metadata-cache costs, so it is reported
# separately as the cold run rather than averaged into the steady-state figure.
# these queries run in well under a second, so the run-to-run spread is on the
# same order as the difference between strategies - hence 2 warmups and 5 timed
# runs, with the spread reported alongside the median so the report can say
# honestly whether a difference is real or noise
WARMUP_RUNS = 2
TIMED_RUNS = 5

# both strategies partition on temporal columns, because borough does not exist
# in the trips data - it only has pulocationid, and borough comes from the zone
# lookup at query time (see the report)
STRATEGIES = {
    "s1_year_month": ["year", "month"],
    "s2_year_month_day": ["year", "month", "day"],
}


def delta_table_stats(path: str) -> tuple:
    """Size and file count of the CURRENT Delta version.

    Deliberately not `du`: mode("overwrite") tombstones the previous version's
    files instead of deleting them, so the directory on disk can be many times
    larger than the live table. Only files still referenced by the transaction
    log are counted.
    """
    live = set()
    for log_file in sorted(glob.glob(os.path.join(path, "_delta_log", "*.json"))):
        with open(log_file) as handle:
            for line in handle:
                action = json.loads(line)
                if "add" in action:
                    live.add(action["add"]["path"])
                if "remove" in action:
                    live.discard(action["remove"]["path"])

    total_bytes = sum(os.path.getsize(os.path.join(path, p)) for p in live)
    partitions = {os.path.dirname(p) for p in live}
    return len(live), total_bytes, len(partitions)


def write_strategy(source: DataFrame, path: str, partition_cols: list) -> float:
    """Write the trips table under one partitioning scheme, returning ingestion seconds.

    Each strategy goes to a fresh directory so no tombstoned files from an
    earlier run can inflate its measured storage size.
    """
    if os.path.exists(path):
        shutil.rmtree(path)

    start = time.perf_counter()
    source.write.format("delta").mode("overwrite").partitionBy(*partition_cols).save(path)
    return time.perf_counter() - start


# --- the three queries required by the assignment -------------------------

def q_trips_per_borough(trips: DataFrame, zones: DataFrame) -> DataFrame:
    return (trips
            .join(zones, trips["pulocationid"] == zones["locationid"], "left")
            .groupBy("borough")
            .agg(count("*").alias("trips")))


def q_avg_duration_per_day(trips: DataFrame, zones: DataFrame) -> DataFrame:
    return (trips
            .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
            .withColumn("duration_min",
                        (unix_timestamp(col("tpep_dropoff_datetime"))
                         - unix_timestamp(col("tpep_pickup_datetime"))) / 60.0)
            .groupBy("pickup_date")
            .agg(avg("duration_min").alias("avg_duration_min")))


def q_avg_fare_per_borough(trips: DataFrame, zones: DataFrame) -> DataFrame:
    return (trips
            .join(zones, trips["pulocationid"] == zones["locationid"], "left")
            .groupBy("borough")
            .agg(avg("fare_amount").alias("avg_fare")))


QUERIES = {
    "trips_per_borough": q_trips_per_borough,
    "avg_duration_per_day": q_avg_duration_per_day,
    "avg_fare_per_borough": q_avg_fare_per_borough,
}


def time_query(spark: SparkSession, table_path: str, query_func: callable) -> dict:
    """Cold and steady-state latency for one query against one stored table.

    The cache is dropped and both DataFrames are re-loaded on every iteration,
    so each run re-reads the table rather than replaying a cached result.
    """
    durations = []
    for _ in range(WARMUP_RUNS + TIMED_RUNS):
        spark.catalog.clearCache()
        trips = spark.read.format("delta").load(table_path)
        zones = spark.read.format("delta").load(SOURCE_ZONES)

        start = time.perf_counter()
        query_func(trips, zones).collect()   # collect() forces full materialisation
        durations.append(time.perf_counter() - start)

    timed = durations[WARMUP_RUNS:]
    return {
        "cold_seconds": round(durations[0], 3),
        "warm_median_seconds": round(statistics.median(timed), 3),
        "warm_min_seconds": round(min(timed), 3),
        "warm_max_seconds": round(max(timed), 3),
        "warm_spread_seconds": round(max(timed) - min(timed), 3),
        "all_runs_seconds": [round(d, 3) for d in durations],
    }


def build_spark() -> SparkSession:
    # same local settings as data_ingestion.py, so the benchmark reflects how the
    # pipeline actually runs rather than a differently tuned session
    spark = SparkSession.builder.appName("Storage_Strategy_Benchmark") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main() -> dict:
    spark = build_spark()

    # the cleaned trips table already carries year and month; day is derived here so
    # that both strategies write identical content and only the partition key differs
    trips_source = spark.read.format("delta").load(SOURCE_TRIPS) \
        .withColumn("day", dayofmonth(col("tpep_pickup_datetime")))

    row_count = trips_source.count()
    print(f"benchmarking {row_count:,} trips under {len(STRATEGIES)} storage strategies\n")

    results = {"row_count": row_count, "warmup_runs": WARMUP_RUNS, "timed_runs": TIMED_RUNS,
               "strategies": {}}

    for name, partition_cols in STRATEGIES.items():
        path = os.path.join(BENCH_ROOT, name)

        ingestion_seconds = write_strategy(trips_source, path, partition_cols)
        files, size_bytes, partitions = delta_table_stats(path)

        strategy_result = {
            "partition_by": partition_cols,
            "ingestion_seconds": round(ingestion_seconds, 2),
            "storage_bytes": size_bytes,
            "storage_mb": round(size_bytes / 1024 / 1024, 1),
            "generated_files": files,
            "partitions": partitions,
            "avg_file_kb": round(size_bytes / files / 1024, 1) if files else 0,
            "queries": {},
        }

        print(f"{name}: partitionBy{tuple(partition_cols)}")
        print(f"  ingestion       {strategy_result['ingestion_seconds']:>8.2f} s")
        print(f"  storage         {strategy_result['storage_mb']:>8.1f} MB")
        print(f"  files           {files:>8}  ({partitions} partitions, "
              f"avg {strategy_result['avg_file_kb']:.1f} KB/file)")

        for query_name, query_func in QUERIES.items():
            timing = time_query(spark, path, query_func)
            strategy_result["queries"][query_name] = timing
            print(f"  {query_name:<22} cold {timing['cold_seconds']:>6.3f} s   "
                  f"warm median {timing['warm_median_seconds']:>6.3f} s   "
                  f"(spread {timing['warm_spread_seconds']:.3f} s)")

        results["strategies"][name] = strategy_result
        print()

    with open(RESULTS_FILE, "w") as handle:
        json.dump(results, handle, indent=4)

    print(f"raw measurements written to {RESULTS_FILE}")
    spark.stop()
    return results


if __name__ == "__main__":
    main()
