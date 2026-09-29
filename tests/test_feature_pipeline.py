"""Tests for the feature engineering pipeline (Week 4, Task 2).

A small dataset shaped like the Task 1 output goes through the real pipeline. Each test checks one
design decision: features are sorted by kind and dropped when the train split cannot teach them,
every estimator learns from the train split only, unseen categories do not break the pipeline,
the fitted pipeline survives a save and load, and a new feature is only a line of configuration.
"""
from dataclasses import replace
from datetime import datetime

import pytest
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

import ml_config
from feature_pipeline import FEATURES, calendar_stage, feature_layout, feature_pipeline

DATASET = replace(ml_config.HOURLY_ZONE_DEMAND,
                  hourly_features={"weather": ("temp", "prcp", "snwd", "coco"), "air_quality": ("pm25",)},
                  lag_hours=(1,))
CONFIG = ml_config.HOURLY_ZONE_DEMAND_FEATURES
COLUMNS = ("pickup_hour timestamp_ntz, pulocationid int, trip_count long, pickup_borough string, "
           "pickup_service_zone string, temp double, prcp double, snwd double, coco int, pm25 double, "
           "wspd double, trip_count_lag_1h long, split string")
ZONES = {1: ("Manhattan", "Yellow Zone"), 2: ("Queens", "Boro Zone"), 3: ("Bronx", "Boro Zone")}


def row(day: datetime, hour: int, zone: int, split: str, prcp_when_present: float = None):
    count = zone * 10 + hour
    prcp = None if hour % 5 == 0 else (prcp_when_present if prcp_when_present is not None else 0.1 * hour)
    return (day.replace(hour=hour), zone, count, *ZONES[zone], float(hour), prcp, None, 3 if hour < 12 else 8,
            8.0, float(hour % 7), None if hour == 0 else count - 1, split)


@pytest.fixture
def dataset(spark):
    """Train: a holiday Monday, a Tuesday and a Saturday in January, zones 1 and 2. Validation: a
    Monday with far wetter hours, plus zone 3, which training never saw."""
    rows = [row(day, hour, zone, "train")
            for day in (datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 6))
            for hour in range(24) for zone in (1, 2)]
    rows += [row(datetime(2024, 1, 8), hour, zone, "validation", prcp_when_present=100.0)
             for hour in range(24) for zone in (1, 2, 3)]
    return spark.createDataFrame(rows, COLUMNS)


def fit(dataset, dataset_config=DATASET):
    train = dataset.where("split = 'train'")
    pipeline, plan = feature_pipeline(train, dataset_config, CONFIG)
    return pipeline.fit(train), plan


def test_features_are_sorted_by_kind_and_what_train_cannot_teach_is_dropped(dataset):
    _, plan = fit(dataset)
    assert plan.numeric == ["temp", "prcp", "trip_count_lag_1h"]
    assert plan.log == ["trip_count_lag_1h"]
    assert plan.missing_flags == ["prcp", "trip_count_lag_1h"]
    assert plan.categorical == ["pulocationid", "pickup_borough", "pickup_service_zone", "coco",
                                "hour_of_day", "day_of_week"]
    assert plan.binary == ["is_weekend", "is_holiday"]
    assert set(plan.dropped) == {"snwd", "pm25", "month"}  # always NULL, always 8.0, always January
    assert "missing in 100%" in plan.dropped["snwd"] and "constant" in plan.dropped["month"]


def test_calendar_features_follow_the_local_date(spark):
    hours = spark.createDataFrame([(datetime(2024, 1, 1, 0),), (datetime(2024, 1, 6, 23),)], "pickup_hour timestamp_ntz")
    new_year, saturday = calendar_stage(CONFIG).transform(hours).orderBy("pickup_hour").collect()
    assert (new_year["day_of_week"], new_year["is_weekend"], new_year["is_holiday"]) == (2, 0.0, 1.0)
    assert (saturday["hour_of_day"], saturday["is_weekend"], saturday["is_holiday"]) == (23, 1.0, 0.0)


def test_estimators_learn_from_train_only_and_an_unseen_zone_encodes_as_zeros(dataset):
    model, plan = fit(dataset)
    transformed = model.transform(dataset)
    layout = [name for name, _ in feature_layout(transformed, plan)]

    # the median of the train split's precipitation, not pulled up by the wet validation hours
    new_zone = transformed.where("pulocationid = 3 AND hour(pickup_hour) = 5").first()
    assert new_zone["prcp"] is None and new_zone["prcp_imputed"] == pytest.approx(1.2)
    assert new_zone["prcp_missing"] == 1.0

    # zone 3 and the Bronx were never seen: no slot of theirs, and no error
    assert "pulocationid=3" not in layout and "pickup_borough=Bronx" not in layout
    vector = new_zone[FEATURES].toArray()
    zone_slots = [i for i, name in enumerate(layout) if name.startswith(("pulocationid=", "pickup_borough="))]
    assert all(vector[i] == 0 for i in zone_slots)

    # quantities are scaled with the train split's mean: 0 there, not over all rows
    temp = layout.index("temp")
    scaled = transformed.select("split", vector_to_array(F.col(FEATURES))[temp].alias("temp"))
    assert scaled.where("split = 'train'").agg(F.avg("temp")).first()[0] == pytest.approx(0, abs=1e-9)


def test_the_fitted_pipeline_is_saved_and_loaded_like_any_spark_model(dataset, tmp_path):
    model, _ = fit(dataset)
    model.write().overwrite().save(str(tmp_path / "pipeline"))
    reloaded = PipelineModel.load(str(tmp_path / "pipeline"))

    features = lambda m: {(r["pickup_hour"], r["pulocationid"]): r[FEATURES] for r in m.transform(dataset).collect()}
    assert features(reloaded) == features(model)


def test_a_new_feature_is_a_line_of_configuration(dataset):
    with_wind = replace(DATASET, hourly_features={"weather": ("temp", "prcp", "snwd", "coco", "wspd"),
                                                  "air_quality": ("pm25",)})
    model, plan = fit(dataset, with_wind)
    assert "wspd" in plan.numeric
    assert "wspd" in [name for name, _ in feature_layout(model.transform(dataset), plan)]
