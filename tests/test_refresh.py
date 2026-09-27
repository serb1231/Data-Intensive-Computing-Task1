"""Tests for analytical consistency (Task 2): the refresh planner and integrated re-enrichment.

A tiny integrated table in a temporary directory is changed in the ways a release changes it
(a new month, a rewritten month, a new column, a lost column, a mis-dated trip), and each test
checks that the planner rebuilds exactly what is affected and that the result equals a
from-scratch build.
"""
from datetime import datetime

import pytest
from pyspark.sql import functions as F

import continuous_data_insert
import data_products
import refresh_manager
from refresh_manager import BLOCKED, FULL, INCREMENTAL, SKIP

COLUMNS = ("tpep_pickup_datetime timestamp, pickup_hour timestamp, trip_distance double, fare_amount double, "
           "total_amount double, passenger_count int, temp double, prcp double, pm25 double, coco int, "
           "pickup_zone string, pickup_borough string, year int, month int")


def trips(year, month, n=3, distance=2.0):
    rows = []
    for i in range(n):
        pickup = datetime(year, month, 1 + i, 8 + i)
        rows.append((pickup, pickup.replace(minute=0), distance + i, 10.0 + i, 12.0 + i, 1 + i % 2,
                     5.0, 0.0, 8.0 + i, 3 if i % 2 else 8, f"Zone {i % 2}", "Manhattan", year, month))
    return rows


@pytest.fixture
def source(spark, platform, monkeypatch):
    """An integrated table with January and February, and products directory, under tmp_path."""
    path = str(platform / "integrated")
    for module in (data_products, refresh_manager):
        monkeypatch.setattr(module, "INTEGRATED_PATH", path)
        monkeypatch.setattr(module, "REGISTRY_PATH", str(platform / "products" / "_registry"))
    monkeypatch.setattr(data_products, "PRODUCTS_DIR", str(platform / "products"))
    monkeypatch.setattr(data_products, "MIN_MONTH_TRIPS", 2)
    monkeypatch.setattr(refresh_manager, "register_views",
                        lambda s: s.read.format("delta").load(path).createOrReplaceTempView("integrated_taxi_trips"))

    write(spark, path, trips(2024, 1) + trips(2024, 2), mode="overwrite")
    return path


def write(spark, path, rows, mode="append", **options):
    writer = spark.createDataFrame(rows, COLUMNS).write.format("delta").mode(mode).partitionBy("year", "month")
    for key, value in options.items():
        writer = writer.option(key, value)
    writer.save(path)


def plan(spark):
    refresh_manager.register_views(spark)
    return {p.name: p for p in refresh_manager.build_plan(spark).products}


def refresh(spark):
    refresh_manager.refresh_affected(spark)


def assert_consistent(spark):
    results = refresh_manager.verify(spark)
    assert all(r["consistent"] for r in results.values()), results


def refreshed_at(spark, name, year, month):
    return (spark.read.format("delta").load(data_products.product_path(name))
            .filter(f"year = {year} AND month = {month}").agg(F.max("refreshed_at")).first()[0])


def test_first_refresh_builds_everything_then_nothing(spark, source):
    assert {p.mode for p in plan(spark).values()} == {FULL}
    refresh(spark)
    assert_consistent(spark)

    second = plan(spark)
    assert {p.mode for p in second.values()} == {SKIP}
    assert "already built" in second["daily_mobility_summary"].reason


def test_new_month_rebuilds_only_that_month(spark, source):
    refresh(spark)
    january = refreshed_at(spark, "taxi_zone_statistics", 2024, 1)

    write(spark, source, trips(2024, 3))
    plans = plan(spark)
    assert {p.mode for p in plans.values()} == {INCREMENTAL}
    assert all(p.months == [(2024, 3)] for p in plans.values())

    refresh(spark)
    assert_consistent(spark)
    assert refreshed_at(spark, "taxi_zone_statistics", 2024, 1) == january  # untouched
    assert refreshed_at(spark, "taxi_zone_statistics", 2024, 3) > january


def test_rewritten_month_is_rebuilt_with_the_corrected_values(spark, source):
    refresh(spark)
    write(spark, source, trips(2024, 2, distance=7.0), mode="overwrite",
          replaceWhere="year = 2024 AND month = 2")

    assert all(p.months == [(2024, 2)] for p in plan(spark).values())
    refresh(spark)
    assert_consistent(spark)


def test_mis_dated_trip_outside_the_window_changes_nothing(spark, source):
    refresh(spark)
    write(spark, source, trips(2023, 12, n=1))  # below MIN_MONTH_TRIPS: not a reporting month

    plans = plan(spark)
    assert {p.mode for p in plans.values()} == {SKIP}
    assert "outside the window (2023-12)" in plans["weather_impact_summary"].reason


def test_new_optional_column_needs_no_full_refresh(spark, source):
    refresh(spark)
    march = spark.createDataFrame(trips(2024, 3), COLUMNS).withColumn("humidity", F.lit(60))
    march.write.format("delta").mode("append").option("mergeSchema", "true").save(source)

    plans = plan(spark)
    assert plans["daily_mobility_summary"].mode == INCREMENTAL
    assert "humidity" in plans["daily_mobility_summary"].reason
    refresh(spark)
    assert_consistent(spark)

    daily = spark.read.format("delta").load(data_products.product_path("daily_mobility_summary"))
    by_month = {r["month"]: r["h"] for r in daily.groupBy("month").agg(F.max("avg_humidity").alias("h")).collect()}
    assert by_month == {1: None, 2: None, 3: 60.0}


def test_type_change_forces_full_and_lost_column_blocks(spark, source):
    refresh(spark)
    table = spark.read.format("delta").load(source)
    (table.withColumn("pm25", F.col("pm25").cast("float"))
     .write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("year", "month")
     .save(source))
    plans = plan(spark)
    assert plans["air_quality_impact_summary"].mode == FULL
    assert "pm25 double -> float" in plans["air_quality_impact_summary"].reason
    assert plans["taxi_zone_statistics"].mode == INCREMENTAL  # does not read pm25

    (table.drop("pm25")
     .write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("year", "month")
     .save(source))
    plans = plan(spark)
    assert plans["air_quality_impact_summary"].mode == BLOCKED
    assert plans["daily_mobility_summary"].mode == BLOCKED
    assert plans["borough_mobility_summary"].mode == INCREMENTAL

    refresh(spark)  # the blocked products keep their last good version; the others are refreshed
    assert data_products.product_exists("air_quality_impact_summary")


# --- re-enrichment of the integrated table ----------------------------------------------------

def test_late_weather_reaches_trips_that_were_already_loaded(spark, platform, monkeypatch):
    trip_table, integrated = str(platform / "trip_data"), str(platform / "integrated")
    monkeypatch.setattr(continuous_data_insert, "TRIP_TABLE", trip_table)
    monkeypatch.setattr(continuous_data_insert, "INTEGRATED_TABLE", integrated)

    pickup = datetime(2024, 4, 2, 9, 15)
    trip_rows = spark.createDataFrame(
        [("k1", 1, pickup, pickup.replace(minute=40), 1, 2, 2024, 4)],
        "surrogate_key string, vendorid int, tpep_pickup_datetime timestamp, "
        "tpep_dropoff_datetime timestamp, pulocationid int, dolocationid int, year int, month int")
    trip_rows.write.format("delta").partitionBy("year", "month").save(trip_table)
    zones = spark.createDataFrame([(1, "Manhattan", "A"), (2, "Queens", "B")], "locationid int, borough string, zone string")
    aq = spark.createDataFrame([(datetime(2024, 4, 2, 9), "PM2.5 - Local Conditions", 7.0)],
                               "timestamp_local timestamp, parameter_name string, sample_measurement double")
    weather_schema = ("timestamp timestamp, temp double, rhum int, prcp double, snwd double, wspd double, "
                      "pres double, coco int")
    no_weather = spark.createDataFrame([], weather_schema)

    from data_ingestion import build_integrated_trips
    build_integrated_trips(trip_rows, no_weather, aq, zones) \
        .write.format("delta").partitionBy("year", "month").save(integrated)
    assert spark.read.format("delta").load(integrated).first()["temp"] is None

    late = spark.createDataFrame([(datetime(2024, 4, 2, 9), 11.5, 40, 0.0, 0.0, 9.0, 1012.0, 2, 45)],
                                 weather_schema + ", humidity int")
    result = continuous_data_insert.reenrich_integrated(spark, {(2024, 4)}, late, aq, zones)

    row = spark.read.format("delta").load(integrated).first()
    assert result["rewritten_records"] == 1
    assert (row["temp"], row["humidity"], row["pm25"]) == (11.5, 45, 7.0)  # humidity: schema evolved
