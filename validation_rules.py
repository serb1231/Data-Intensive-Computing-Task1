"""Week 3, Task 4: the validation rules of every dataset, as configuration.

This file holds no validation logic. It declares, per dataset, what the platform expects:
which source columns exist (the schema in schemas.py, including announced additions such as
humidity and aqi), which of them are mandatory, and the dataset-specific record rules.

The generic rules are added by the framework to every dataset and are not listed here:
  well_formed                        every value could be read as its declared type
  not_null(pk), unique(pk)           the primary key is present and unique within the batch
  not_already_loaded(pk)             the record is not a re-delivery of one already loaded

To add a rule, append it to the dataset's list. A rule type that does not exist yet is a
small Rule subclass with a violated() method (see validation.py). The engine never changes.
The Week 1 cleaning rules that used to live in modify_data_generic are marked "week 1" below.
"""
from schemas import air_quality_schema, taxi_zones_schema, trips_schema, weather_schema
from validation import (MISSING_REFERENCE, WARN, AllowedValues, Check, DatasetContract, InRange,
                        NotNull, Positive, ReferenceExists, not_null)

TAXI_ZONES_PATH = "output_data/taxi_zones"

CONTRACTS = {
    "Weather": DatasetContract(
        schema=weather_schema,
        required_columns=["year", "month", "day", "hour", "temp"],
        rules=[
            # week 1 also required the timestamp: it is the primary key, so the generic rules cover it
            NotNull("temp"),
            Positive("year"),                                    # week 1
            InRange("month", 1, 12), InRange("day", 1, 31),      # week 1 only required > 0
            InRange("hour", 0, 23),
            # plausibility bounds, wide enough for any New York weather on record
            InRange("temp", -40, 50),                            # deg C
            InRange("rhum", 0, 100), InRange("humidity", 0, 100),  # percent
            InRange("prcp", 0, 300),                             # mm per hour
            InRange("snwd", 0, 3000),                            # mm
            InRange("wdir", 0, 360),                             # degrees
            InRange("wspd", 0, 250), InRange("wpgt", 0, 350),    # km/h
            InRange("pres", 870, 1090),                          # hPa
            InRange("cldc", 0, 8),                               # okta
            InRange("coco", 1, 27),                              # Meteostat condition codes
        ],
    ),

    "Air Quality": DatasetContract(
        schema=air_quality_schema,
        required_columns=["state_code", "county_code", "site_num", "parameter_code", "poc",
                          "parameter_name", "date_local", "time_local", "date_gmt", "time_gmt",
                          "sample_measurement", "method_name", "date_of_last_change"],
        rules=[
            # week 1: the three timestamps must parse
            *not_null("timestamp_local", "timestamp_gmt", "timestamp_last_change", "sample_measurement"),
            # EPA accepts slightly negative PM2.5 readings from continuous monitors (instrument noise)
            InRange("sample_measurement", -10, 1000),
            InRange("aqi", 0, 500),
            InRange("latitude", -90, 90), InRange("longitude", -180, 180),
            Check("gmt_offset_plausible",
                  "abs(unix_timestamp(timestamp_gmt) - unix_timestamp(timestamp_local)) <= 14 * 3600"),
        ],
    ),

    "Taxi Zones": DatasetContract(
        schema=taxi_zones_schema,
        required_columns=["locationid", "borough", "zone"],
        rules=[
            Positive("locationid"),                              # week 1
            *not_null("borough", "zone"),
        ],
    ),

    "Trip Data": DatasetContract(
        schema=trips_schema,
        required_columns=["vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
                          "trip_distance", "pulocationid", "dolocationid", "fare_amount", "total_amount"],
        rules=[
            # week 1: timestamps present, passenger_count and trip_distance present and > 0
            *not_null("tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
                      "pulocationid", "dolocationid"),
            Positive("passenger_count"), Positive("trip_distance"),
            InRange("trip_distance", max=500),                   # miles; longer "trips" are meter errors
            Check("dropoff_after_pickup", "tpep_dropoff_datetime >= tpep_pickup_datetime"),
            Check("trip_under_24h", "tpep_dropoff_datetime <= tpep_pickup_datetime + INTERVAL 24 HOURS"),
            Check("pickup_after_2009", "year(tpep_pickup_datetime) >= 2009"),  # TLC records start in 2009
            # negative amounts are refunds and voids: financial adjustments rather than trips
            Check("non_negative_amounts", "fare_amount >= 0 AND total_amount >= 0"),
            # codes from the TLC yellow taxi data dictionary
            AllowedValues("vendorid", [1, 2, 6, 7]),
            AllowedValues("ratecodeid", [1, 2, 3, 4, 5, 6, 99]),
            AllowedValues("payment_type", [0, 1, 2, 3, 4, 5, 6]),
            AllowedValues("store_and_fwd_flag", ["Y", "N"]),
            ReferenceExists("pulocationid", TAXI_ZONES_PATH, "locationid"),
            ReferenceExists("dolocationid", TAXI_ZONES_PATH, "locationid"),
        ],
    ),

    # derived from already-validated trips, so the key checks are inherited rather than repeated.
    # A trip without weather or air quality for its hour is still a valid trip, so these only warn.
    "Integrated Taxi Trips": DatasetContract(
        check_primary_key=False,
        skip_already_loaded=False,
        rules=[
            NotNull("temp", name="weather_observation_exists", category=MISSING_REFERENCE, action=WARN),
            NotNull("pm25", name="air_quality_observation_exists", category=MISSING_REFERENCE, action=WARN),
        ],
    ),
}


def get_contract(dataset: str) -> DatasetContract:
    """A dataset nobody has written a contract for yet still gets the generic rules."""
    return CONTRACTS.get(dataset, DatasetContract())
