"""Week 4: the configuration of the machine learning pipeline.

This file holds no logic. It declares what the training dataset contains: the prediction
problem, the columns taken from the platform, the time window, the lags and the splits (Task 1),
and how the feature pipeline treats each column (Task 2). training_dataset.py and
feature_pipeline.py build whatever is declared here, so adding an hourly feature, a lag, a
calendar feature or another week of data is a change to this file, not to the code.
"""
from dataclasses import dataclass, field
from typing import Optional

ML_DIR = "output_data/ml"


@dataclass(frozen=True)
class TrainingDatasetConfig:
    # also the directory of the dataset under output_data/ml/
    name: str
    target: str

    # [window_start, window_end) as 'YYYY-MM-DD', in New York local time. None means the first,
    # or the last, month the platform reports on (data_products.discover_window)
    window_start: Optional[str]
    window_end: Optional[str]

    # chronological splits in whole weeks, counted back from window_end; train is everything before
    validation_weeks: int
    test_weeks: int

    # integrated-table columns that describe the hour rather than the trip, by the dataset they came
    # from: the platform joined them on the pickup hour, so every trip in an hour carries the same value
    hourly_features: dict

    # taxi_zones column -> dataset column
    zone_features: dict

    # features whose values are codes rather than quantities
    categorical_features: tuple

    # which locations are places: 264 "Unknown" and 265 "Outside of NYC" are not
    zone_filter: str

    # zones that averaged fewer pickups per day over the training weeks are left out
    min_zone_trips_per_day: float

    # trips the platform quarantined for these validation rules and no other still count as pickups
    # (read from output_data/quarantine/trip_data). Empty: only the trips that passed validation count
    count_quarantined_for: tuple = field(default=())

    # the zone's demand this many hours earlier on the local clock (24 = same hour yesterday)
    lag_hours: tuple = field(default=())

    @property
    def path(self) -> str:
        return f"{ML_DIR}/{self.name}"

    @property
    def hourly_columns(self) -> list:
        return [column for columns in self.hourly_features.values() for column in columns]

    @property
    def lag_columns(self) -> list:
        return [f"{self.target}_lag_{hours}h" for hours in self.lag_hours]

    @property
    def feature_columns(self) -> list:
        return ["pulocationid", *self.zone_features.values(), *self.hourly_columns, *self.lag_columns]


# --- Task 1: hourly taxi demand per pickup zone ------------------------------------------
#
# One row per (pickup zone, local hour): how many trips started in the zone during that hour.
HOURLY_ZONE_DEMAND = TrainingDatasetConfig(
    name="hourly_zone_demand",
    target="trip_count",

    # the window ends with March: April 2024 is the Week 3 simulated update, copies of Q1 trips whose
    # pickup times were drawn uniformly over nine days. It has no daily cycle, so it is not demand.
    # January 1, 2024 is a Monday, so the 13 weeks split into whole weeks: 9 train, 2 + 2 held out.
    window_start=None,
    window_end="2024-04-01",
    validation_weeks=2,
    test_weeks=2,

    # weather: temperature, relative humidity, precipitation, wind speed, pressure, condition code.
    # Snow depth (snwd) is left out: the station reported none in Q1 2024. humidity and aqi, added by
    # the Week 3 release, duplicate rhum and a rescaled pm25; add them here once they carry new values.
    hourly_features={
        "weather": ("temp", "rhum", "prcp", "wspd", "pres", "coco"),
        "air_quality": ("pm25",),
    },

    # the zone name is left out: it is the same information as pulocationid
    zone_features={"borough": "pickup_borough", "service_zone": "pickup_service_zone"},
    categorical_features=("pulocationid", "pickup_borough", "pickup_service_zone", "coco"),
    zone_filter="service_zone <> 'N/A'",
    min_zone_trips_per_day=1.0,

    # Week 3 validation quarantines a trip whose passenger count is missing or 0. The trip still
    # happened, and leaving it out is not a constant factor: 4% of Q1 pickups at noon but 26% at 4 am,
    # 5% in January but 13% in March. Trips quarantined for these rules only are counted as pickups.
    count_quarantined_for=("not_null(passenger_count)", "positive(passenger_count)"),

    lag_hours=(1, 24, 168),
)

TRAINING_DATASET = HOURLY_ZONE_DEMAND

# measurements of the latest build (row counts, split boundaries, null shares, signal per feature);
# committed next to the Week 1 and Week 3 results
MANIFEST_PATH = "training_dataset_week_4.json"


@dataclass(frozen=True)
class FeaturePipelineConfig:
    # also the directory of the feature dataset under output_data/ml/ and of the fitted pipeline
    # under output_data/ml/models/
    name: str

    # features derived from the local hour: column -> Spark SQL expression over pickup_hour
    calendar_features: dict

    # the derived features that are codes (one-hot encoded) and those that are 0/1 flags (used as
    # they are). Every other derived or dataset feature not declared categorical is a quantity
    categorical_calendar: tuple
    binary_features: tuple

    # counts spanning orders of magnitude: log1p before they are imputed and scaled
    log_features: tuple

    # a feature missing in a larger share of the train split than this, or constant in it, is dropped
    max_null_share: float

    @property
    def path(self) -> str:
        return f"{ML_DIR}/{self.name}"

    @property
    def model_path(self) -> str:
        return f"{ML_DIR}/models/{self.name}"


# --- Task 2: the feature pipeline for hourly zone demand -----------------------------------

# US federal holidays of 2024; extend the list when the data reaches another year
HOLIDAYS = ("2024-01-01", "2024-01-15", "2024-02-19", "2024-05-27", "2024-06-19", "2024-07-04",
            "2024-09-02", "2024-10-14", "2024-11-11", "2024-11-28", "2024-12-25")

HOURLY_ZONE_DEMAND_FEATURES = FeaturePipelineConfig(
    name="hourly_zone_demand_features",

    calendar_features={
        "hour_of_day": "hour(pickup_hour)",
        "day_of_week": "dayofweek(pickup_hour)",  # 1 = Sunday, 7 = Saturday
        "month": "month(pickup_hour)",
        "is_weekend": "CAST(dayofweek(pickup_hour) IN (1, 7) AS DOUBLE)",
        "is_holiday": "CAST(to_date(pickup_hour) IN ({}) AS DOUBLE)".format(
            ", ".join(f"DATE '{day}'" for day in HOLIDAYS)),
    },
    # the hour and the weekday as codes: demand is not a straight line in either
    categorical_calendar=("hour_of_day", "day_of_week"),
    binary_features=("is_weekend", "is_holiday"),

    # the lags run from 0 in a quiet zone to several hundred in Midtown
    log_features=tuple(HOURLY_ZONE_DEMAND.lag_columns),
    max_null_share=0.5,
)

FEATURE_PIPELINE = HOURLY_ZONE_DEMAND_FEATURES

# the plan, what the fitted stages learned, the feature layout and the per-source probe
FEATURE_MANIFEST_PATH = "feature_pipeline_week_4.json"
