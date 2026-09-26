import json
import re
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import concat_ws, col, lpad, lit, to_timestamp, md5, year, month, date_trunc, avg
from schemas import *
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import os

import monitoring
from validation import (DROP_COLUMN, EVOLVE, ValidationContext, align_to_target, check_source_schema,
                        check_target_schema, conform_types, data_schema, files_for, normalize_column_name,
                        schema_change_results, validate, validation_enabled, write_quarantine)
from validation_rules import get_contract

def readDataGeneric(spark_session:SparkSession, path, data_type, schema:StructType) -> DataFrame:
    # load the data
    if data_type == "csv":
        # read every column as text and bind it by its header name, not by its position: with a
        # positional schema, a column inserted or reordered in a new release would silently shift
        # values into the wrong columns. The declared types are applied below by conform_types,
        # which records values it cannot convert instead of turning them into NULL.
        data : DataFrame = spark_session.read.csv(path, sep=',', header=True, inferSchema=False, mode="PERMISSIVE")
    elif data_type == "parquet":
        data : DataFrame = spark_session.read.parquet(path)
    else:
        raise ValueError("Unsupported file type!")

    # respect naming conventions
    for column in data.columns:
        data = data.withColumnRenamed(column, normalize_column_name(column))

    if data_type == "csv" and schema is not None:
        data = conform_types(data, {normalize_column_name(f.name): f.dataType for f in schema.fields})

    return data

def weather_data_process(weather_data_raw: DataFrame) -> DataFrame:
    # normalize data timestamps
    # apply data specific transformation rules
    weather_data_raw = weather_data_raw.withColumn(
        "timestamp",
        to_timestamp(concat_ws(
            " ",
            concat_ws("-", col("year"), lpad(col("month"), 2, "0"), lpad(col("day"), 2, "0")),
            concat_ws(":", lpad(col("hour"), 2, "0"), lit("00"), lit("00"))
        ), "yyyy-MM-dd HH:mm:ss")
    )

    return weather_data_raw

# the EPA extract is nationwide; the taxi trips are NYC-only, and joining on
# local time is only valid within a single timezone, so scope it to the boroughs
NY_STATE_CODE = "36"
NYC_COUNTY_CODES = ["005", "047", "081"]  # Bronx, Kings/Brooklyn, Queens (no station in Manhattan or Staten Island)

def air_quality_scope(air_quality_raw: DataFrame) -> DataFrame:
    # scoping, not cleaning: these rows are valid EPA data, they are simply about
    # other parts of the country. Kept separate from air_quality_process so the
    # ingestion metadata does not report them as quality rejections.
    return air_quality_raw.filter(
        (col("state_code") == NY_STATE_CODE) & (col("county_code").isin(NYC_COUNTY_CODES))
    )

def air_quality_process(air_quality_raw: DataFrame) -> DataFrame:
    air_quality_raw = air_quality_raw.withColumn("timestamp_local",
                                                   to_timestamp(concat_ws(" ", col("date_local"),
                                                                          concat_ws(":", col("time_local"), lit("00"))),
                                                                "yyyy-MM-dd HH:mm:ss")
                                                   )

    air_quality_raw = air_quality_raw.withColumn("timestamp_gmt",
                                                   to_timestamp(concat_ws(" ", col("date_gmt"),
                                                                          concat_ws(":", col("time_gmt"), lit("00"))),
                                                                "yyyy-MM-dd HH:mm:ss")
                                                   )

    air_quality_raw = air_quality_raw.withColumn("timestamp_last_change",
                                                   to_timestamp(
                                                       concat_ws(" ", col("date_of_last_change"), lit("00:00:00")),
                                                       "yyyy-MM-dd HH:mm:ss"
                                                       )
                                                   )

    air_quality_raw = air_quality_raw.withColumn("surrogate_key", md5(concat_ws("||", col('state_code'), col('county_code'), col('parameter_code'), col('site_num'),col('date_gmt'), col('time_gmt'), col('method_name'), col('poc'))))

    return air_quality_raw

def trip_data_process(trip_data_combined_raw: DataFrame) -> DataFrame:
    trip_data_combined_raw = trip_data_combined_raw.withColumn(
        "surrogate_key",
        md5(concat_ws("||",
                      col("vendorid"),
                      col("tpep_pickup_datetime"),
                      col("tpep_dropoff_datetime"),
                      col("pulocationid")
                      ))
    )

    trip_data_combined_raw = trip_data_combined_raw.withColumn("year", year(col("tpep_pickup_datetime").cast(TimestampType()))) \
                                                    .withColumn("month", month(col("tpep_pickup_datetime").cast(TimestampType())))

    return trip_data_combined_raw

def taxi_zones_data_process(taxi_zones_data_raw: DataFrame) -> DataFrame:
    # nothing to derive: the lookup is loaded as delivered and checked by its validation rules
    return taxi_zones_data_raw


def execute_pipeline(dataset_name: str, raw_df: DataFrame, process_func: callable, output_path: str,
                     pk_col: str, partition_cols: list = None, scope_func: callable = None) -> tuple:
    spark_session = raw_df.sparkSession
    contract = get_contract(dataset_name)
    source_schema = data_schema(raw_df)

    # every execution is recorded in the monitoring tables, including rejected and failed ones
    with monitoring.track(spark_session, dataset_name, output_path, raw_df, source_schema) as execution:
        start_time = time.time()
        target_exists = DeltaTable.isDeltaTable(spark_session, output_path)
        target_schema = spark_session.read.format("delta").load(output_path).schema if target_exists else None

        # dataset level: does this release still match the contract? Undeclared columns are
        # removed here; a missing required column rejects the batch before anything is computed
        schema_changes = check_source_schema(contract, source_schema)
        raw_df = raw_df.drop(*[c.column for c in schema_changes if c.action == DROP_COLUMN])
        source_count = raw_df.count()
        if any(c.blocking for c in schema_changes):
            return _reject_batch(execution, dataset_name, spark_session, raw_df, output_path, target_exists,
                                 schema_changes, source_count, start_time)

        if scope_func:
            raw_df = scope_func(raw_df)

        # process functions only derive columns; every filtering decision is a validation rule
        processed_df = process_func(raw_df)
        if target_exists:
            schema_changes += check_target_schema(data_schema(processed_df), target_schema)
            if any(c.blocking for c in schema_changes):
                return _reject_batch(execution, dataset_name, spark_session, raw_df, output_path, target_exists,
                                     schema_changes, source_count, start_time)
            processed_df = align_to_target(processed_df, target_schema, schema_changes)

        # record level: every rule over one materialized pass of the batch; rejected rows are isolated
        validate_start = time.time()
        rules = contract.all_rules(pk_col, processed_df) if validation_enabled() else []
        outcome = validate(processed_df, rules,
                           ValidationContext(spark_session, dataset_name, output_path, target_exists))
        if outcome.quarantined:
            write_quarantine(outcome.quarantined_records(), outcome.quarantined, dataset_name,
                             execution.run_id, execution.execution_id)
        validate_seconds = time.time() - validate_start

        # kept materialized: callers build the integrated table from the clean trips batch
        clean_df = outcome.valid()
        clean_count = outcome.valid_count

        write_start = time.time()
        if not target_exists:
            # first run: table doesn't exist yet, so create it. The validated batch is cached, and
            # AQE may not coalesce a cached plan's partitions, so without the repartition every one
            # of the 64 shuffle partitions would write a file into every table partition it holds
            # (7,372 files for 8,784 weather rows instead of one file per day).
            if partition_cols:
                writer = clean_df.repartition(*partition_cols).write.format("delta").partitionBy(*partition_cols)
            else:
                writer = clean_df.coalesce(files_for(clean_count)).write.format("delta")
            writer.save(output_path)

            write_mode = "created"
            inserted_count = clean_count
        elif clean_count == 0:
            # nothing survived validation: skip the merge rather than commit an empty table version
            write_mode = "unchanged"
            inserted_count = 0
        else:
            if any(c.action == EVOLVE for c in schema_changes):
                # only columns the contract declares get this far (undeclared ones were dropped above)
                spark_session.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

            # table exists: insert only the rows whose key is not already there
            target = DeltaTable.forPath(spark_session, output_path)
            (target.alias("t")
                   .merge(clean_df.alias("s"), f"t.{pk_col} = s.{pk_col}")
                   .whenNotMatchedInsertAll()
                   .execute())

            # Delta records how many rows the merge actually inserted
            metrics = target.history(1).select("operationMetrics").collect()[0][0]
            write_mode = "merged"
            inserted_count = int(metrics.get("numTargetRowsInserted", 0))
        target_version = DeltaTable.forPath(spark_session, output_path).history(1).collect()[0]["version"]

        end_time = time.time()
        checks = schema_change_results(schema_changes) + outcome.results
        metadata = {
            "dataset": dataset_name,
            "run_id": execution.run_id,
            "status": "succeeded",
            "schema_version": execution.schema_version,
            "schema_changes": "; ".join(str(c) for c in schema_changes) or None,
            "write_mode": write_mode,
            "source_records": source_count,
            "out_of_scope_records": source_count - outcome.total,
            "processed_records": outcome.total,
            "rejected_records": outcome.quarantined + outcome.dropped,
            "quarantined_records": outcome.quarantined,
            "dropped_records": outcome.dropped,
            "warning_records": outcome.warned,
            "validation_failures": sum(1 for c in checks if c.status == "failed"),
            "final_clean_records": clean_count,
            "inserted_records": inserted_count,
            "duplicates_skipped": clean_count - inserted_count,
            "target_version": target_version,
            "validate_seconds": round(validate_seconds, 2),
            "write_seconds": round(end_time - write_start, 2),
            "execution_time_seconds": round(end_time - start_time, 2)
        }
        execution.record(metadata, checks)

    print(json.dumps(metadata, indent=4))
    return clean_df, metadata


def _reject_batch(execution, dataset_name, spark_session, raw_df, output_path, target_exists,
                  schema_changes, source_count, start_time) -> tuple:
    """A schema change the platform cannot absorb: write nothing, record why, and carry on.

    The batch stays where it was delivered (that file is its quarantine). The caller gets an
    empty DataFrame shaped like the table, so downstream steps such as the integrated table
    simply process zero new rows instead of failing.
    """
    checks = schema_change_results(schema_changes)
    metadata = {
        "dataset": dataset_name,
        "run_id": execution.run_id,
        "status": "rejected",
        "schema_version": execution.schema_version,
        "schema_changes": "; ".join(str(c) for c in schema_changes),
        "write_mode": "rejected",
        "source_records": source_count,
        "out_of_scope_records": 0,
        "processed_records": source_count,
        "rejected_records": source_count,
        "quarantined_records": 0,
        "dropped_records": 0,
        "warning_records": 0,
        "validation_failures": sum(1 for c in checks if c.status == "failed"),
        "final_clean_records": 0,
        "inserted_records": 0,
        "duplicates_skipped": 0,
        "execution_time_seconds": round(time.time() - start_time, 2)
    }
    execution.record(metadata, checks)
    print(json.dumps(metadata, indent=4))
    empty = spark_session.read.format("delta").load(output_path) if target_exists else raw_df
    return empty.limit(0), metadata

# friendly short names for common EPA parameter names; any pollutant not
# listed here falls back to a normalized slug of its raw name
AQ_PARAMETER_FRIENDLY_NAMES = {
    "PM2.5 - Local Conditions": "pm25",
    "PM10 - Local Conditions": "pm10",
    "Ozone": "ozone",
    "Carbon monoxide": "co",
    "Nitrogen dioxide (NO2)": "no2",
    "Sulfur dioxide": "so2",
}

# Task 5
def build_integrated_trips(trips, weather, air_quality, taxi_zones):
    # truncate trip pickup to the hour (temporal join key)
    trips = trips.withColumn("pickup_hour", date_trunc("hour", col("tpep_pickup_datetime")))

    # weather: select only measurements, avoid year/month/day/hour collision
    weather_sel = weather.select(
        col("timestamp").alias("weather_hour"),
        col("temp"), col("rhum"), col("prcp"),
        col("snwd"), col("wspd"), col("pres"), col("coco")
    )

    # air quality: collapse to ONE row per hour, pivoted per parameter
    # (keeps each pollutant in its own units instead of averaging across units)
    aq_hourly = (air_quality
        .withColumn("aq_hour", date_trunc("hour", col("timestamp_local")))
        .groupBy("aq_hour")
        .pivot("parameter_name")
        .agg(avg("sample_measurement")))

    # pollutant names become column names dynamically (pivot); use a friendly
    # short name for known EPA parameters, else fall back to a normalized slug
    for column in aq_hourly.columns:
        if column != "aq_hour":
            col_renamed = AQ_PARAMETER_FRIENDLY_NAMES.get(
                column, re.sub(r"[^0-9a-z]+", "_", column.strip().lower()).strip("_")
            )
            aq_hourly = aq_hourly.withColumnRenamed(column, col_renamed)

    # for zones, join the same lookup twice, aliased for pickup vs dropoff
    pu_zones = taxi_zones.select(
        col("locationid").alias("pu_locationid"),
        col("borough").alias("pickup_borough"),
        col("zone").alias("pickup_zone"))
    do_zones = taxi_zones.select(
        col("locationid").alias("do_locationid"),
        col("borough").alias("dropoff_borough"),
        col("zone").alias("dropoff_zone"))

    # all left joins so trips (the primary entity) are never dropped
    integrated = (trips
        .join(weather_sel, col("pickup_hour") == col("weather_hour"), "left")
        .join(aq_hourly,  col("pickup_hour") == col("aq_hour"),      "left")
        .join(pu_zones,   col("pulocationid") == col("pu_locationid"), "left")
        .join(do_zones,   col("dolocationid") == col("do_locationid"), "left"))

    return integrated

def build_spark() -> SparkSession:
    # turn down the local parallelism in order to not consume all RAM
    builder = SparkSession.builder.appName('Generic_Ingestion_Framework') \
        .master("local[4]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark

def main():

    spark = build_spark()
    monitoring.start_run("initial_load")

    weather_raw = readDataGeneric(spark, "data/weather.csv", "csv", weather_schema)
    air_quality_raw = readDataGeneric(spark, "data/air_quality.csv", "csv", air_quality_schema)
    taxi_zones_raw = readDataGeneric(spark, "data/taxi_zone_lookup.csv", "csv", taxi_zones_schema)

    trip_1 = readDataGeneric(spark, "data/yellow_tripdata_2024-01.parquet", "parquet", trips_schema)
    trip_2 = readDataGeneric(spark, "data/yellow_tripdata_2024-02.parquet", "parquet", trips_schema)
    trip_3 = readDataGeneric(spark, "data/yellow_tripdata_2024-03.parquet", "parquet", trips_schema)
    trip_combined_raw = trip_1.unionByName(trip_2).unionByName(trip_3)

    weather_clean, weather_meta = execute_pipeline(
        "Weather", weather_raw, weather_data_process, "output_data/weather",
        pk_col="timestamp",
        partition_cols=["year", "month", "day"]
    )

    air_quality_clean, aq_meta = execute_pipeline(
        "Air Quality", air_quality_raw, air_quality_process, "output_data/air_quality",
        pk_col="surrogate_key",
        partition_cols=None,
        scope_func=air_quality_scope
    )

    taxi_zones_clean, tz_meta = execute_pipeline(
        "Taxi Zones", taxi_zones_raw, taxi_zones_data_process, "output_data/taxi_zones",
        pk_col="locationid",
        partition_cols=None
    )

    trip_clean, trip_meta = execute_pipeline(
        "Trip Data", trip_combined_raw, trip_data_process, "output_data/trip_data",
        pk_col="surrogate_key",
        partition_cols=["year", "month"]
    )

    print(json.dumps({
        "weather_metadata": weather_meta,
        "air_quality_metadata": aq_meta,
        "taxi_zones_metadata": tz_meta,
        "trip_data_metadata": trip_meta
    }, indent=4))

    # task 5: build and save the integrated table
    integrated_clean, integrated_meta = execute_pipeline(
        "Integrated Taxi Trips",
        trip_clean,
        lambda df: build_integrated_trips(df, weather_clean, air_quality_clean, taxi_zones_clean),
        "output_data/integrated_taxi_trips",
        pk_col="surrogate_key",
        partition_cols=["year", "month"]
    )
    # sanity check: integrated count should equal trip count (left joins, no fan-out)
    print("trips:", trip_clean.count(), "integrated:", integrated_clean.count())

    integrated_clean.select(
        "tpep_pickup_datetime", "temp", "pm25",
        "pickup_borough", "dropoff_borough"
    ).show(10, truncate=False)

if __name__ == "__main__":
    main()