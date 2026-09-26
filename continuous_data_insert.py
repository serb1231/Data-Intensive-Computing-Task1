from data_ingestion import (
    readDataGeneric, execute_pipeline,
    weather_data_process, air_quality_process, air_quality_scope,
    trip_data_process, build_integrated_trips, build_spark
)
from schemas import weather_schema, air_quality_schema
import monitoring

AIR_QUALITY_PATH = "continuous_data/air_quality_continuous.csv"
TRIP_DATA_PATH = "continuous_data/tripdata_merged_no_duplicates_continuous_sorted.parquet"
WEATHER_PATH = "continuous_data/weather_continuous.csv"


def main():
    spark = build_spark()
    monitoring.start_run("incremental_update")

    weather_raw = readDataGeneric(spark, WEATHER_PATH, "csv", weather_schema)
    air_qual_raw = readDataGeneric(spark, AIR_QUALITY_PATH, "csv", air_quality_schema)
    trip_raw = readDataGeneric(spark, TRIP_DATA_PATH, "parquet", None)

    # weather data insertion
    execute_pipeline("Weather", weather_raw, weather_data_process,
                     "output_data/weather", pk_col="timestamp",
                     partition_cols=["year", "month", "day"])

    # air quality insertion
    execute_pipeline("Air Quality", air_qual_raw, air_quality_process,
                     "output_data/air_quality", pk_col="surrogate_key",
                     partition_cols=None, scope_func=air_quality_scope)

    # trip data insertion
    trip_clean, _ = execute_pipeline("Trip Data", trip_raw, trip_data_process,
                                     "output_data/trip_data", pk_col="surrogate_key",
                                     partition_cols=["year", "month"])

    # integrated table
    load = lambda p: spark.read.format("delta").load(p)
    weather_full = load("output_data/weather")
    aq_full = load("output_data/air_quality")
    zones_full = load("output_data/taxi_zones")

    execute_pipeline("Integrated Taxi Trips", trip_clean,
                     lambda df: build_integrated_trips(df, weather_full, aq_full, zones_full),
                     "output_data/integrated_taxi_trips", pk_col="surrogate_key",
                     partition_cols=["year", "month"])


if __name__ == "__main__":
    main()