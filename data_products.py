import argparse
import json
import os
import time
from datetime import datetime

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-memory 4g "
    "--packages io.delta:delta-spark_2.12:3.1.0 "
    "pyspark-shell"
)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

from analytical_queries import (INTEGRATED_PATH, WEATHER_CATEGORY_SQL,
                                build_spark, register_views)

# every data product is a Delta table under this directory, next to the Week 1
# tables in output_data/ + one sub-directory per product, plus the registry
PRODUCTS_DIR = "output_data/data_products"
REGISTRY_PATH = f"{PRODUCTS_DIR}/_registry"

SOURCE_TABLE = "integrated_taxi_trips"
SCHEMA_VERSION = "1.0"

VALID_YEAR = 2024
VALID_MONTHS = (1, 2, 3)


def register_report_view(spark: SparkSession, months=VALID_MONTHS, use_cache=True) -> None:
    """Register `trips`: the scoped, narrowed slice of the integrated table that every product reads."""
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW trips AS
        SELECT tpep_pickup_datetime, pickup_hour,
               trip_distance, fare_amount, total_amount, passenger_count,
               temp, prcp, pm25,
               pickup_zone, pickup_borough,
               year, month,
               {WEATHER_CATEGORY_SQL} AS weather_category
        FROM {SOURCE_TABLE}
        WHERE year = {VALID_YEAR} AND month IN ({", ".join(str(m) for m in months)})
    """)

    # Two deliberate choices here, both worth measuring in Task 5:
    #  1. the view selects 14 of the 38 columns -- the products need nothing else, and a
    #     narrower projection means less to read and far less to keep in memory;
    #  2. CACHE TABLE materializes that slice once. All five products aggregate the same
    #     rows, so without it Spark re-scans the 577 MB table five times.
    if use_cache:
        spark.sql("CACHE TABLE trips")


# --- the five data products -------------------------------------------------
#
# Design rule used by all of them: store ADDITIVE measures (trip_count, total_*)
# next to the convenient averages. An average cannot be re-aggregated correctly
# (the mean of per-zone means is not the citywide mean), but a sum can, so storing
# the sums is what lets an analyst roll a product up to a coarser grain and still
# get the right number.


def build_daily_mobility_summary(spark: SparkSession) -> DataFrame:
    """One row per calendar day: how the city moved, with the weather and air it moved in."""
    return spark.sql("""
        SELECT to_date(tpep_pickup_datetime)             AS service_date,
               date_format(tpep_pickup_datetime, 'EEEE') AS day_of_week,
               COUNT(*)                                  AS trip_count,
               ROUND(SUM(trip_distance), 2)              AS total_distance_miles,
               ROUND(SUM(total_amount), 2)               AS total_revenue,
               ROUND(AVG(trip_distance), 3)              AS avg_trip_distance,
               ROUND(AVG(total_amount), 2)               AS avg_trip_revenue,
               ROUND(AVG(passenger_count), 2)            AS avg_passengers,
               -- weather/air columns are trip-weighted on purpose: this is the weather the
               -- average rider actually travelled in, not the station's unweighted daily mean
               ROUND(AVG(temp), 1)                       AS avg_temp_c,
               ROUND(AVG(prcp), 2)                       AS avg_precip_mm,
               ROUND(AVG(pm25), 1)                       AS avg_pm25,
               year, month
        FROM trips
        GROUP BY service_date, day_of_week, year, month
        ORDER BY service_date
    """)


def build_taxi_zone_statistics(spark: SparkSession) -> DataFrame:
    """One row per (month, pickup zone): demand, revenue and the zone's rank within the city."""
    return spark.sql("""
        WITH zone_month AS (
            SELECT year, month, pickup_zone, pickup_borough,
                   COUNT(*)                     AS trip_count,
                   ROUND(SUM(trip_distance), 2) AS total_distance_miles,
                   ROUND(SUM(total_amount), 2)  AS total_revenue,
                   ROUND(AVG(trip_distance), 3) AS avg_trip_distance,
                   ROUND(AVG(fare_amount), 2)   AS avg_fare
            FROM trips
            WHERE pickup_zone IS NOT NULL
            GROUP BY year, month, pickup_zone, pickup_borough
        )
        -- rank and share are computed once here so the dashboard does not have to
        -- re-run a window function over the whole city every time it draws the table
        SELECT *,
               RANK() OVER (PARTITION BY year, month ORDER BY trip_count DESC) AS demand_rank,
               ROUND(100.0 * trip_count / SUM(trip_count) OVER (PARTITION BY year, month), 3)
                   AS pct_of_monthly_trips
        FROM zone_month
        ORDER BY year, month, demand_rank
    """)


def build_weather_impact_summary(spark: SparkSession) -> DataFrame:
    """One row per (weather category, pickup zone) -- serves query 2 and query 4 from one table.

    Rolled up over zones it answers "how far do people travel in the rain?" (query 2);
    grouped by zone it answers "which zones are most weather-sensitive?" (query 4).
    """
    return spark.sql("""
        SELECT weather_category, pickup_zone, pickup_borough,
               COUNT(*)                        AS trip_count,
               ROUND(SUM(trip_distance), 2)    AS total_distance_miles,
               ROUND(SUM(total_amount), 2)     AS total_revenue,
               ROUND(AVG(trip_distance), 3)    AS avg_trip_distance,
               ROUND(STDDEV(trip_distance), 3) AS stddev_trip_distance
        FROM trips
        WHERE weather_category <> 'Unknown' AND pickup_zone IS NOT NULL
        GROUP BY weather_category, pickup_zone, pickup_borough
        ORDER BY weather_category, trip_count DESC
    """)


def build_air_quality_impact_summary(spark: SparkSession) -> DataFrame:
    """One row per hour: citywide PM2.5 next to the demand recorded in that hour."""
    return spark.sql("""
        SELECT pickup_hour,
               ROUND(AVG(pm25), 2) AS pm25,
               -- EPA AQI bands for PM2.5; stored as a column so the banded breakdown is a
               -- GROUP BY over ~2k rows instead of a re-scan of 8.5M trips
               CASE WHEN AVG(pm25) < 12   THEN 'Good'
                    WHEN AVG(pm25) < 35.4 THEN 'Moderate'
                    ELSE 'Unhealthy' END AS aqi_band,
               COUNT(*)                     AS trip_count,
               ROUND(SUM(trip_distance), 2) AS total_distance_miles,
               ROUND(AVG(trip_distance), 3) AS avg_trip_distance,
               year, month
        FROM trips
        WHERE pm25 IS NOT NULL
        GROUP BY pickup_hour, year, month
        ORDER BY pickup_hour
    """)


def build_borough_mobility_summary(spark: SparkSession) -> DataFrame:
    """One row per (borough, weekday, hour) -- the weekly demand profile of each borough."""
    return spark.sql("""
        SELECT pickup_borough,
               date_format(tpep_pickup_datetime, 'EEEE') AS day_of_week,
               dayofweek(tpep_pickup_datetime)           AS day_of_week_num,
               hour(tpep_pickup_datetime)                AS hour_of_day,
               COUNT(*)                                  AS trip_count,
               ROUND(SUM(trip_distance), 2)              AS total_distance_miles,
               ROUND(SUM(total_amount), 2)               AS total_revenue,
               ROUND(AVG(trip_distance), 3)              AS avg_trip_distance
        FROM trips
        WHERE pickup_borough IS NOT NULL
        GROUP BY pickup_borough, day_of_week, day_of_week_num, hour_of_day
        ORDER BY pickup_borough, day_of_week_num, hour_of_day
    """)


# name -> how to build it, how to store it.
# Only the daily summary is partitioned: it is the one product that grows with time and
# can be refreshed one month at a time. The other four are a few hundred rows each, and
# partitioning a table that small only creates tiny files that make reads slower.
PRODUCTS = {
    "daily_mobility_summary": {
        "build": build_daily_mobility_summary,
        "partition_by": ["year", "month"],
        "incremental": True,
    },
    "taxi_zone_statistics": {
        "build": build_taxi_zone_statistics,
        "partition_by": None,
        "incremental": False,
    },
    "weather_impact_summary": {
        "build": build_weather_impact_summary,
        "partition_by": None,
        "incremental": False,
    },
    "air_quality_impact_summary": {
        "build": build_air_quality_impact_summary,
        "partition_by": None,
        "incremental": False,
    },
    "borough_mobility_summary": {
        "build": build_borough_mobility_summary,
        "partition_by": None,
        "incremental": False,
    },
}


# --- metadata ---------------------------------------------------------------

def delta_sql_name(path: str) -> str:
    """SQL name for a Delta table stored as plain files, e.g. delta.`/abs/path`.

    The path has to be absolute: Spark resolves `delta`.`relative/path` as a catalog
    name and reports the table as not found.
    """
    return f"delta.`{os.path.abspath(path)}`"


def source_delta_version(spark: SparkSession) -> int:
    """Version of the integrated table the products are being built from (DESCRIBE HISTORY lists newest first)."""
    history = spark.sql(f"DESCRIBE HISTORY {delta_sql_name(INTEGRATED_PATH)} LIMIT 1")
    return int(history.first()["version"])


def product_created_at(spark: SparkSession, path: str, default: datetime) -> datetime:
    """When this product was first created: the timestamp of commit 0 in its own Delta log.

    Creation time is a property of the table, not of this run, so it is read back from the
    table rather than recomputed. On the very first build the table does not exist yet, and
    then this refresh IS the creation.
    """
    if not os.path.isdir(os.path.join(path, "_delta_log")):
        return default
    return spark.sql(f"DESCRIBE DETAIL {delta_sql_name(path)}").first()["createdAt"]


def add_metadata_columns(df: DataFrame, source_version: int,
                         created_at: datetime, refreshed_at: datetime) -> DataFrame:
    """Stamp every row with where it came from, when the table was created and when it was refreshed.

    These five constant columns are exactly what Task 4 asks a data product to carry: the data
    source (and the source version it was built from), the schema version, the creation time and
    the refresh time. Constant columns look wasteful but cost almost nothing on disk (Parquet
    dictionary-encodes a repeated value), and they travel with the data: an analyst who exports
    the table to a spreadsheet still knows how old it is and which source version it reflects.
    """
    return (df
            .withColumn("source_table", lit(SOURCE_TABLE))
            .withColumn("source_version", lit(source_version))
            .withColumn("schema_version", lit(SCHEMA_VERSION))
            .withColumn("created_at", lit(created_at).cast(TimestampType()))
            .withColumn("refreshed_at", lit(refreshed_at).cast(TimestampType())))


REGISTRY_SCHEMA = StructType([
    StructField("product_name", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("source_version", IntegerType(), False),
    StructField("schema_version", StringType(), False),
    StructField("created_at", TimestampType(), True),
    StructField("refreshed_at", TimestampType(), False),
    StructField("row_count", LongType(), False),
    StructField("size_bytes", LongType(), False),
    StructField("num_files", IntegerType(), False),
    StructField("build_seconds", DoubleType(), False),
    StructField("partitioned_by", StringType(), True),
    StructField("refresh_mode", StringType(), False),
])


def append_registry_row(spark: SparkSession, row: dict) -> None:
    """Append one row to the registry table, creating it if it does not exist yet."""
    values = [tuple(row[field.name] for field in REGISTRY_SCHEMA.fields)]
    spark.createDataFrame(values, REGISTRY_SCHEMA) \
        .write.format("delta").mode("append").save(REGISTRY_PATH)


#  writing 
def write_product(spark: SparkSession, name: str, df: DataFrame, source_version: int,
                  replace_where: str = None) -> dict:
    """Materialize one product as a Delta table and return its registry row."""
    spec = PRODUCTS[name]
    path = f"{PRODUCTS_DIR}/{name}"
    refreshed_at = datetime.now()
    created_at = product_created_at(spark, path, refreshed_at)
    start = time.time()

    stamped = add_metadata_columns(df, source_version, created_at, refreshed_at)

    # coalesce(1): the products are small, and spark.sql.shuffle.partitions=64 would
    # otherwise scatter a few hundred rows over 64 files. One file per table (per
    # partition) is both smaller on disk and faster to open.
    writer = stamped.coalesce(1).write.format("delta").mode("overwrite")

    # the same metadata, attached to the Delta commit itself, so `DESCRIBE HISTORY`
    # on the product explains where each version came from without the registry
    writer = writer.option("userMetadata", json.dumps({
        "product": name,
        "source_table": SOURCE_TABLE,
        "source_version": source_version,
        "schema_version": SCHEMA_VERSION,
        "created_at": created_at.isoformat(timespec="seconds"),
        "refreshed_at": refreshed_at.isoformat(timespec="seconds"),
    }))

    if replace_where:
        # incremental refresh: overwrite only the partitions matching this predicate and
        # leave every other month untouched (overwriteSchema is not allowed with it)
        writer = writer.option("replaceWhere", replace_where)
    else:
        writer = writer.option("overwriteSchema", "true")

    if spec["partition_by"]:
        writer = writer.partitionBy(*spec["partition_by"])

    writer.save(path)
    build_seconds = round(time.time() - start, 2)

    # DESCRIBE DETAIL reports the CURRENT version of the table: how big it is and how many
    # files it is stored in, which is what the registry records as the product's storage cost.
    detail = spark.sql(f"DESCRIBE DETAIL {delta_sql_name(path)}").first()
    row_count = spark.read.format("delta").load(path).count()

    return {
        "product_name": name,
        "source_table": SOURCE_TABLE,
        "source_version": source_version,
        "schema_version": SCHEMA_VERSION,
        "created_at": created_at,
        "refreshed_at": refreshed_at,
        "row_count": row_count,
        "size_bytes": int(detail["sizeInBytes"]),
        "num_files": int(detail["numFiles"]),
        "build_seconds": build_seconds,
        "partitioned_by": ",".join(spec["partition_by"]) if spec["partition_by"] else None,
        "refresh_mode": "incremental" if replace_where else "full",
    }


def refresh(spark: SparkSession, names: list, source_version: int, replace_where: str = None) -> list:
    rows = []
    for name in names:
        print(f"\n--- building {name} ---")
        df = PRODUCTS[name]["build"](spark)
        row = write_product(spark, name, df, source_version, replace_where)
        append_registry_row(spark, row)
        print(json.dumps({k: str(v) for k, v in row.items()}, indent=4))
        spark.read.format("delta").load(f"{PRODUCTS_DIR}/{name}").show(5, truncate=False)
        rows.append(row)
    return rows


# --- demonstration: the products answering the Week 2 queries ---------------

def show_products_in_use(spark: SparkSession) -> None:
    """Re-answer three of the Task 1 analyses straight off the products, to show they are enough."""
    for name in PRODUCTS:
        spark.read.format("delta").load(f"{PRODUCTS_DIR}/{name}").createOrReplaceTempView(name)

    print("\n=== Query 2 from weather_impact_summary (rolled up over zones) ===")
    # note the SUM/SUM: averaging the stored averages would weight a 40-trip zone the
    # same as a 400k-trip one. This is why the product stores additive measures.
    spark.sql("""
        SELECT weather_category,
               SUM(trip_count) AS trip_count,
               ROUND(SUM(total_distance_miles) / SUM(trip_count), 3) AS avg_distance
        FROM weather_impact_summary
        GROUP BY weather_category
        ORDER BY avg_distance DESC
    """).show(truncate=False)

    print("\n=== Query 4 from weather_impact_summary (grouped by zone) ===")
    spark.sql("""
        SELECT pickup_zone,
               SUM(trip_count) AS total_trips,
               ROUND(STDDEV(trip_count) / AVG(trip_count), 4) AS coeff_of_variation
        FROM weather_impact_summary
        GROUP BY pickup_zone
        HAVING SUM(trip_count) >= 500 AND COUNT(*) >= 2
        ORDER BY coeff_of_variation DESC
        LIMIT 10
    """).show(truncate=False)

    print("\n=== Query 5 from borough_mobility_summary (citywide peak hour per weekday) ===")
    spark.sql("""
        WITH citywide AS (
            SELECT day_of_week, day_of_week_num, hour_of_day, SUM(trip_count) AS trip_count
            FROM borough_mobility_summary
            GROUP BY day_of_week, day_of_week_num, hour_of_day
        ),
        ranked AS (
            SELECT *, RANK() OVER (PARTITION BY day_of_week ORDER BY trip_count DESC) AS rnk
            FROM citywide
        )
        SELECT day_of_week, hour_of_day, trip_count FROM ranked WHERE rnk = 1
        ORDER BY day_of_week_num
    """).show(truncate=False)


def show_registry(spark: SparkSession) -> None:
    print("\n=== data product registry (latest refresh per product) ===")
    spark.read.format("delta").load(REGISTRY_PATH).createOrReplaceTempView("registry")
    spark.sql("""
        WITH latest AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY product_name ORDER BY refreshed_at DESC) AS rn
            FROM registry
        )
        SELECT product_name, source_table, source_version, schema_version,
               created_at, refreshed_at, row_count, size_bytes, num_files,
               build_seconds, partitioned_by, refresh_mode
        FROM latest WHERE rn = 1
        ORDER BY product_name
    """).show(truncate=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the reusable analytical data products (Week 2, Task 4).")
    parser.add_argument("--product", default="all", choices=["all"] + list(PRODUCTS),
                        help="which product to refresh (default: all)")
    parser.add_argument("--month", metavar="YYYY-MM",
                        help="incremental refresh of one month (daily_mobility_summary only)")
    parser.add_argument("--no-cache", action="store_true",
                        help="skip CACHE TABLE, to measure what caching is worth")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = build_spark()
    register_views(spark)

    if args.month:
        # one month only: rebuild just that partition of the daily summary and leave the
        # other months in place. The full-window products cannot be refreshed this way --
        # a city-wide rank or a per-zone standard deviation needs all the months at once.
        year, month = (int(part) for part in args.month.split("-"))
        if year != VALID_YEAR or month not in VALID_MONTHS:
            raise SystemExit(f"--month must be one of {VALID_YEAR}-01, {VALID_YEAR}-02, {VALID_YEAR}-03")
        names = ["daily_mobility_summary"]
        months = (month,)
        replace_where = f"year = {year} AND month = {month}"
        print(f"incremental refresh of {names[0]} for {args.month}")
    else:
        names = list(PRODUCTS) if args.product == "all" else [args.product]
        months = VALID_MONTHS
        replace_where = None

    register_report_view(spark, months, use_cache=not args.no_cache)
    source_version = source_delta_version(spark)
    print(f"source: {SOURCE_TABLE} (Delta version {source_version}), window {VALID_YEAR} months {months}")

    refresh(spark, names, source_version, replace_where)

    if not args.month and args.product == "all":
        show_products_in_use(spark)
    show_registry(spark)

    spark.stop()


if __name__ == "__main__":
    main()
