"""Tests for the training dataset (Week 4, Task 1).

A handful of trips per hour go through the real builder, and each test checks one design decision:
empty zone-hours are zeros, lags follow the local clock, zones are chosen on the training weeks,
the splits are whole weeks, and the hour key does not depend on the Spark session time zone.
"""
from dataclasses import replace
from datetime import date, datetime, timedelta

import pytest

import data_products
import ml_config
from data_products import partition_predicate
from training_dataset import build_training_dataset, quarantined_pickups, resolve_window, split_boundaries

TRIPS = "tpep_pickup_datetime timestamp_ntz, pulocationid int, temp double, pm25 double, coco int, year int, month int"
ZONES = "locationid int, borough string, zone string, service_zone string"
QUARANTINE = "tpep_pickup_datetime timestamp_ntz, pulocationid int, surrogate_key string, _rejection_reasons array<string>"
ZONE_ROWS = [(1, "Manhattan", "A", "Yellow Zone"), (2, "Queens", "B", "Boro Zone"),
             (3, "Bronx", "C", "Boro Zone"), (264, "Unknown", "N/A", "N/A")]

CONFIG = replace(ml_config.HOURLY_ZONE_DEMAND, window_start=None, window_end=None,
                 hourly_features={"weather": ("temp", "coco"), "air_quality": ("pm25",)},
                 validation_weeks=1, test_weeks=1, lag_hours=(1, 24))


def trip(pickup: datetime, zone: int = 1):
    # the weather of an hour is recognisable by its temperature: the hour of the day
    return pickup, zone, float(pickup.hour), 8.0, 3, pickup.year, pickup.month


def every_hour(start: datetime, end: datetime, zone: int = 1, skip=()) -> list:
    """One trip at half past every hour of [start, end), except the hours in skip."""
    hours = int((end - start) / timedelta(hours=1))
    return [trip(start + timedelta(hours=h, minutes=30), zone) for h in range(hours)
            if start + timedelta(hours=h) not in skip]


def build(spark, rows, start: date, end: date, config=CONFIG) -> dict:
    trips = spark.createDataFrame(rows, TRIPS)
    zones = spark.createDataFrame(ZONE_ROWS, ZONES)
    dataset = build_training_dataset(trips, zones, config, start, end).collect()
    return {(r["pulocationid"], r["pickup_hour"]): r for r in dataset}


@pytest.fixture
def january(spark):
    """Three weeks: zone 1 busy every hour, zone 2 eight trips in the first week, zone 3 only in the
    last week, and the Unknown zone busy but not a place."""
    start, end = datetime(2024, 1, 1), datetime(2024, 1, 22)
    rows = every_hour(start, end, zone=1) + every_hour(start, end, zone=264)
    rows += [trip(datetime(2024, 1, 2, hour, 10), zone=2) for hour in range(8, 16)]
    rows += [trip(datetime(2024, 1, 20, hour, 10), zone=3) for hour in range(24)]
    return build(spark, rows, start.date(), end.date())


def test_every_zone_has_every_hour_and_an_empty_hour_is_zero(january):
    assert {zone for zone, _ in january} == {1, 2}  # 3: no pickups in the training week; 264: not a place
    assert len(january) == 2 * 21 * 24

    quiet = january[(2, datetime(2024, 1, 2, 7))]
    assert quiet["trip_count"] == 0
    assert quiet["temp"] == 7.0  # the hour's weather, although no trip in the zone carried it
    assert quiet["pickup_borough"] == "Queens" and quiet["pickup_service_zone"] == "Boro Zone"
    assert january[(2, datetime(2024, 1, 2, 8))]["trip_count"] == 1


def test_lags_read_the_zero_filled_series_and_are_null_before_the_data(january):
    assert january[(2, datetime(2024, 1, 2, 9))]["trip_count_lag_1h"] == 1
    assert january[(2, datetime(2024, 1, 2, 8))]["trip_count_lag_1h"] == 0  # a quiet hour, not a missing one
    assert january[(2, datetime(2024, 1, 3, 8))]["trip_count_lag_24h"] == 1
    assert january[(1, datetime(2024, 1, 1, 0))]["trip_count_lag_1h"] is None
    assert january[(1, datetime(2024, 1, 1, 23))]["trip_count_lag_24h"] is None


def test_trips_quarantined_only_for_their_passenger_count_are_pickups(spark):
    start, end = datetime(2024, 1, 1), datetime(2024, 1, 22)
    at = datetime(2024, 1, 2, 8, 20)
    quarantine = spark.createDataFrame([
        (at, 1, "a", ["not_null(passenger_count)"]),
        (at, 1, "a", ["not_null(passenger_count)"]),  # appended again when the release was re-run
        (at, 1, "b", ["positive(passenger_count)"]),
        (at, 1, "c", ["not_null(passenger_count)", "positive(trip_distance)"]),  # suspect for another reason
        (at, 1, "d", ["non_negative_amounts", "unique(surrogate_key)"]),
    ], QUARANTINE)
    trips = spark.createDataFrame(every_hour(start, end), TRIPS)
    zones = spark.createDataFrame(ZONE_ROWS, ZONES)
    pickups = quarantined_pickups(quarantine, CONFIG.count_quarantined_for)
    dataset = build_training_dataset(trips, zones, CONFIG, start.date(), end.date(), pickups).collect()
    rows = {r["pickup_hour"]: r for r in dataset}

    assert rows[datetime(2024, 1, 2, 8)]["trip_count"] == 3  # the validated trip, "a" once and "b"
    assert rows[datetime(2024, 1, 2, 8)]["temp"] == 8.0  # a quarantined trip brings no weather of its own
    assert rows[datetime(2024, 1, 2, 9)]["trip_count_lag_1h"] == 3


def test_splits_are_whole_weeks_counted_back_from_the_end(january):
    splits = {}
    for (_, hour), row in january.items():
        splits.setdefault(row["split"], set()).add(hour)
    assert {name: (min(hours), len(hours)) for name, hours in splits.items()} == {
        "train": (datetime(2024, 1, 1), 168),
        "validation": (datetime(2024, 1, 8), 168),
        "test": (datetime(2024, 1, 15), 168),
    }
    with pytest.raises(ValueError, match="less than a week"):
        split_boundaries(CONFIG, date(2024, 1, 1), date(2024, 1, 20))


def test_hours_follow_the_new_york_clock_whatever_the_session_time_zone(spark):
    # New York skipped 2024-03-10 02:00; Europe/Stockholm skipped 2024-03-31 02:00, which in New York
    # is an ordinary hour. date_trunc in a Stockholm session would count its trips at 03:00.
    zone = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "Europe/Stockholm")
    try:
        start, end = datetime(2024, 3, 4), datetime(2024, 4, 1)
        rows = every_hour(start, end, skip={datetime(2024, 3, 10, 2)})
        dataset = build(spark, rows, start.date(), end.date(),
                        replace(CONFIG, validation_weeks=1, test_weeks=1))
    finally:
        spark.conf.set("spark.sql.session.timeZone", zone)

    assert len(dataset) == 4 * 7 * 24 - 1
    assert dataset[(1, datetime(2024, 3, 31, 2))]["trip_count"] == 1
    assert dataset[(1, datetime(2024, 3, 31, 3))]["trip_count"] == 1
    assert (1, datetime(2024, 3, 10, 2)) not in dataset
    assert dataset[(1, datetime(2024, 3, 10, 3))]["trip_count_lag_1h"] is None  # that hour never existed
    assert dataset[(1, datetime(2024, 3, 10, 4))]["trip_count_lag_1h"] == 1


def test_window_and_lag_history_leave_out_mis_dated_months(spark, monkeypatch):
    monkeypatch.setattr(data_products, "MIN_MONTH_TRIPS", 100)
    rows = [trip(datetime(2023, 12, 31, 23, 40))]  # a mis-dated trip: a month of its own with one trip
    rows += every_hour(datetime(2024, 1, 1), datetime(2024, 2, 12, 5))
    integrated = spark.createDataFrame(rows, TRIPS)

    start, end, months = resolve_window(spark, integrated, CONFIG)
    assert (start, end, months) == (date(2024, 1, 1), date(2024, 2, 13), [(2024, 1), (2024, 2)])
    start, end, _ = resolve_window(spark, integrated, replace(CONFIG, window_start="2023-06-01",
                                                              window_end="2024-02-05"))
    assert (start, end) == (date(2024, 1, 1), date(2024, 2, 5))

    zones = spark.createDataFrame(ZONE_ROWS, ZONES)
    trips = integrated.where(partition_predicate(months))
    first = build_training_dataset(trips, zones, CONFIG, start, end).orderBy("pickup_hour").first()
    assert first["pickup_hour"] == datetime(2024, 1, 1) and first["trip_count_lag_1h"] is None

    # a window that starts later takes its first lags from the hours before it
    later = build_training_dataset(trips, zones, CONFIG, date(2024, 1, 8), end).orderBy("pickup_hour").first()
    assert later["trip_count_lag_24h"] == 1
