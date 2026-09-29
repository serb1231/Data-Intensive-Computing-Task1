"""Week 4: the configuration of the machine learning pipeline.

This file holds no logic. It declares what the training dataset contains: the prediction
problem, the columns taken from the platform, the time window, the lags and the splits.
training_dataset.py (Task 1) builds whatever is declared here, so adding an hourly feature,
a lag or another week of data is a change to this file, not to the builder.
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
