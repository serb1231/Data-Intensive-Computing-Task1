import json
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import concat_ws, col, lpad, lit, to_timestamp, md5, year, month
from schemas import *
import os

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-memory 4g "
    "--packages io.delta:delta-spark_2.12:3.1.0 "
    "pyspark-shell"
)

def readDataGeneric(spark_session:SparkSession, path, data_type, schema:StructType) -> DataFrame:
    # load the data
    if data_type == "csv":
        # using FAILFAST is the schema validator
        data : DataFrame = spark_session.read.csv(path, sep=',', schema=schema, header=True, mode="FAILFAST")
    elif data_type == "parquet":
        data : DataFrame = spark_session.read.parquet(path)
    else:
        raise ValueError("Unsupported file type!")

    # respect naming conventions
    for column in data.columns:
        col_renamed = column.strip().lower().replace(" ", "_")

        data = data.withColumnRenamed(column, col_renamed)

    return data

def modify_data_generic(data: DataFrame, columns_int_higher_0: list[str], columns_timestamps: list[str], columns_pk: list[str]) -> DataFrame:
    for column in columns_int_higher_0:
        data = data.filter(col(column) > 0)

    for column in columns_timestamps:
        data = data.filter(col(column).isNotNull())

    for column in columns_pk:
        data = data.filter(col(column).isNotNull())

    if columns_pk:
        data = data.drop_duplicates(columns_pk)

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

    weather_data_raw = modify_data_generic(
        weather_data_raw,
        columns_int_higher_0=["year", "month", "day"],
        columns_timestamps=["timestamp"],
        columns_pk=["timestamp"]
    )

    return weather_data_raw

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

    air_quality_raw = modify_data_generic(
        air_quality_raw,
        columns_int_higher_0=[],
        columns_timestamps=["timestamp_local", "timestamp_gmt", "timestamp_last_change"],
        columns_pk=["surrogate_key"]
    )

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

    trip_data_combined_raw = modify_data_generic(
        trip_data_combined_raw,
        columns_int_higher_0=["passenger_count", "trip_distance"],
        columns_timestamps=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
        columns_pk=["surrogate_key"]
    )

    return trip_data_combined_raw

def taxi_zones_data_process(taxi_zones_data_raw: DataFrame) -> DataFrame:
    taxi_zones_data_raw = modify_data_generic(
        taxi_zones_data_raw,
        columns_int_higher_0=["locationid"],
        columns_timestamps=[],
        columns_pk=["locationid"]
    )

    return taxi_zones_data_raw


def execute_pipeline(dataset_name: str, raw_df: DataFrame, process_func: callable, output_path: str,
                     partition_cols: list = None) -> tuple:
    # time metadata
    start_time = time.time()

    raw_count = raw_df.count()

    # apply processing function to clean the data
    clean_df = process_func(raw_df)

    # save to data lake
    writer = clean_df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(output_path)

    clean_count = clean_df.count()

    # make metadata
    end_time = time.time()
    metadata = {
        "dataset": dataset_name,
        "schema_version": "1.0",
        "processed_records": raw_count,
        "rejected_records": raw_count - clean_count,
        "final_clean_records": clean_count,
        "execution_time_seconds": round(end_time - start_time, 2)
    }

    print(json.dumps(metadata, indent=4))

    return clean_df, metadata


# turn down the local parallelism in order to not consume all RAM
spark = SparkSession.builder.appName('Generic_Ingestion_Framework') \
    .master("local[4]") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


weather_raw = readDataGeneric(spark, "data/weather.csv", "csv", weather_schema)
air_quality_raw = readDataGeneric(spark, "data/air_quality.csv", "csv", air_quality_schema)
taxi_zones_raw = readDataGeneric(spark, "data/taxi_zone_lookup.csv", "csv", taxi_zones_schema)

trip_1 = readDataGeneric(spark, "data/yellow_tripdata_2024-01.parquet", "parquet", trips_schema)
trip_2 = readDataGeneric(spark, "data/yellow_tripdata_2024-02.parquet", "parquet", trips_schema)
trip_3 = readDataGeneric(spark, "data/yellow_tripdata_2024-03.parquet", "parquet", trips_schema)
trip_combined_raw = trip_1.unionByName(trip_2).unionByName(trip_3)

weather_clean, weather_meta = execute_pipeline(
    "Weather", weather_raw, weather_data_process, "output_data/weather", ["year", "month", "day"]
)

air_quality_clean, aq_meta = execute_pipeline(
    "Air Quality", air_quality_raw, air_quality_process, "output_data/air_quality"
)

taxi_zones_clean, tz_meta = execute_pipeline(
    "Taxi Zones", taxi_zones_raw, taxi_zones_data_process, "output_data/taxi_zones"
)

trip_clean, trip_meta = execute_pipeline(
    "Trip Data", trip_combined_raw, trip_data_process, "output_data/trip_data", ["year", "month"]
)

print(json.dumps({
    "weather_metadata": weather_meta,
    "air_quality_metadata": aq_meta,
    "taxi_zones_metadata": tz_meta,
    "trip_data_metadata": trip_meta
}, indent=4))
