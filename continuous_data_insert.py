import argparse
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, month, year

from data_ingestion import (readDataGeneric, execute_pipeline,
                            weather_data_process, air_quality_process, air_quality_scope,
                            trip_data_process, build_integrated_trips, build_spark)
from data_products import delta_sql_name, format_months, partition_predicate
from schemas import weather_schema, air_quality_schema
from validation import align_to_target, check_target_schema, data_schema
import monitoring

AIR_QUALITY_PATH = "continuous_data/air_quality_continuous.csv"
TRIP_DATA_PATH = "continuous_data/tripdata_merged_no_duplicates_continuous_sorted.parquet"
WEATHER_PATH = "continuous_data/weather_continuous.csv"

INTEGRATED_TABLE = "output_data/integrated_taxi_trips"
TRIP_TABLE = "output_data/trip_data"


def months_of(df: DataFrame, timestamp_col: str = None) -> set:
    #(year, month) pairs present in a batch.
    if timestamp_col:
        df = df.select(year(col(timestamp_col)).alias("year"), month(col(timestamp_col)).alias("month"))
    rows = df.select("year", "month").distinct().collect()
    return {(r["year"], r["month"]) for r in rows if r["year"] is not None}


def load_updates(spark: SparkSession) -> dict:
    #Steps 1-3. Returns the per-dataset metadata and wall-clock time (used by evaluate_platform.py).
    load = lambda p: spark.read.format("delta").load(p)
    timings = {}

    def timed(name, *args, **kwargs):
        start = time.time()
        clean, metadata = execute_pipeline(name, *args, **kwargs)
        timings[name] = {**metadata, "wall_seconds": round(time.time() - start, 2)}
        return clean

    # the months the integrated table already covers: only these can have stale enrichment
    loaded_months = months_of(load(INTEGRATED_TABLE))

    weather_raw = readDataGeneric(spark, WEATHER_PATH, "csv", weather_schema)
    air_qual_raw = readDataGeneric(spark, AIR_QUALITY_PATH, "csv", air_quality_schema)
    trip_raw = readDataGeneric(spark, TRIP_DATA_PATH, "parquet", None)

    weather_clean = timed("Weather", weather_raw, weather_data_process,
                          "output_data/weather", pk_col="timestamp",
                          partition_cols=["year", "month", "day"])

    aq_clean = timed("Air Quality", air_qual_raw, air_quality_process,
                     "output_data/air_quality", pk_col="surrogate_key",
                     partition_cols=None, scope_func=air_quality_scope)

    trip_clean = timed("Trip Data", trip_raw, trip_data_process,
                       TRIP_TABLE, pk_col="surrogate_key",
                       partition_cols=["year", "month"])

    # integrated table: the new trips, enriched with the weather and air quality as they are now
    weather_full = load("output_data/weather")
    aq_full = load("output_data/air_quality")
    zones_full = load("output_data/taxi_zones")

    timed("Integrated Taxi Trips", trip_clean,
          lambda df: build_integrated_trips(df, weather_full, aq_full, zones_full),
          INTEGRATED_TABLE, pk_col="surrogate_key",
          partition_cols=["year", "month"])

    observed = months_of(weather_clean) | months_of(aq_clean, "timestamp_local")
    start = time.time()
    reenriched = reenrich_integrated(spark, observed & loaded_months, weather_full, aq_full, zones_full)
    timings["Integrated re-enrichment"] = {**reenriched, "wall_seconds": round(time.time() - start, 2)}
    return timings


def reenrich_integrated(spark: SparkSession, months: set, weather: DataFrame, air_quality: DataFrame,
                        zones: DataFrame) -> dict:

    if not months:
        print("\nre-enrichment: the new weather and air-quality observations cover no month that "
              "already has trips; nothing to rebuild")
        return {"months": "-", "rewritten_records": 0}

    predicate = partition_predicate(months)
    trips = spark.read.format("delta").load(TRIP_TABLE).filter(predicate)
    with monitoring.track(spark, "Integrated Taxi Trips", INTEGRATED_TABLE, trips, data_schema(trips)) as execution:
        start = time.time()
        rebuilt = build_integrated_trips(trips, weather, air_quality, zones)
        target_schema = spark.read.format("delta").load(INTEGRATED_TABLE).schema
        changes = check_target_schema(data_schema(rebuilt), target_schema)
        rebuilt = align_to_target(rebuilt, target_schema, changes)
        added = [c.column for c in changes if c.action == "evolve"]
        (rebuilt.select(*target_schema.fieldNames(), *added)
                .write.format("delta").mode("overwrite")
                .option("replaceWhere", predicate)
                .option("mergeSchema", "true")
                .partitionBy("year", "month")
                .save(INTEGRATED_TABLE))
        history = spark.sql(f"DESCRIBE HISTORY {delta_sql_name(INTEGRATED_TABLE)} LIMIT 1").first()
        rewritten = int(history["operationMetrics"].get("numOutputRows", 0))
        metadata = {
            "status": "succeeded",
            "schema_changes": "; ".join(str(c) for c in changes) or None,
            "write_mode": "reenriched",
            "processed_records": rewritten,
            "final_clean_records": rewritten,
            "inserted_records": 0,
            "rejected_records": 0,
            "validation_failures": 0,
            "target_version": int(history["version"]),
            "execution_time_seconds": round(time.time() - start, 2),
        }
        execution.record(metadata, [])
    print(f"\nre-enrichment: rebuilt {format_months(months)} of the integrated table ({rewritten:,} trips)")
    return {"months": format_months(months), "rewritten_records": rewritten}


def main():
    parser = argparse.ArgumentParser(description="Load the incremental release (Week 3).")
    parser.add_argument("--no-refresh", action="store_true", help="do not refresh the data products")
    args = parser.parse_args()

    spark = build_spark()
    monitoring.start_run("incremental_update")
    load_updates(spark)

    if not args.no_refresh:
        # imported here: refresh_manager pulls in the product definitions, which the load does not need
        import refresh_manager
        refresh_manager.refresh_affected(spark)


if __name__ == "__main__":
    main()
