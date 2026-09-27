"""The reusable analytical data products (Week 2, Task 4), kept consistent in Week 3, Task 2.

Every product is a Delta table under output_data/data_products/, built from the integrated
table and partitioned by (year, month). Week 3 changed three things:

1. Every product now has a per-month grain. A month's rows depend only on that month's trips,
   so a month can be rebuilt on its own with a Delta replaceWhere and the other months stay
   untouched. The two products whose Week 2 grain spanned all months (weather_impact_summary,
   borough_mobility_summary) gained year and month columns plus unrounded sums, and their
   Week 2 shape is served by a compatibility view (see register_product_views).
2. The reporting window is discovered from the data (discover_window) instead of being the
   hard-coded January to March 2024, so a new month is picked up when it arrives.
3. Each product declares the source columns it reads (required and optional), so the refresh
   planner in refresh_manager.py can tell which products a schema change affects.

`python data_products.py` rebuilds every product in full, as in Week 2. After an incremental
load, `python refresh_manager.py` rebuilds only the months and products that changed.
"""
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

from analytical_queries import INTEGRATED_PATH, WEATHER_CATEGORY_SQL, register_views

# every data product is a Delta table under this directory, next to the Week 1
# tables in output_data/ + one sub-directory per product, plus the registry
PRODUCTS_DIR = "output_data/data_products"
REGISTRY_PATH = f"{PRODUCTS_DIR}/_registry"

SOURCE_TABLE = "integrated_taxi_trips"
PARTITION_COLS = ["year", "month"]
METADATA_COLS = ["source_table", "source_version", "schema_version", "created_at", "refreshed_at"]

# a (year, month) enters the reporting window once it holds this many trips. The integrated
# table keeps a handful of mis-dated trips (4 in 2009-01, 10 in 2023-12); a month made of those
# would show up as a real month with a -99.9% demand drop in every trend.
MIN_MONTH_TRIPS = 1000


def product_path(name: str) -> str:
    return f"{PRODUCTS_DIR}/{name}"


# --- the reporting window ---------------------------------------------------

def discover_window(spark: SparkSession, source: DataFrame = None) -> list:
    """The (year, month) pairs the products report on: every month with enough trips.

    Only the partition columns are read, so Spark answers this from the Parquet footers
    without scanning the data.
    """
    source = source if source is not None else spark.read.format("delta").load(INTEGRATED_PATH)
    counts = source.groupBy(*PARTITION_COLS).count().collect()
    return sorted((row["year"], row["month"]) for row in counts
                  if row["year"] is not None and row["count"] >= MIN_MONTH_TRIPS)


def partition_predicate(months) -> str:
    """SQL predicate selecting the given (year, month) pairs, usable as a Delta replaceWhere."""
    by_year = {}
    for year, month in sorted(months):
        by_year.setdefault(year, []).append(month)
    if not by_year:
        return "FALSE"
    return " OR ".join(f"(year = {year} AND month IN ({', '.join(str(m) for m in months_of_year)}))"
                       for year, months_of_year in by_year.items())


def format_months(months) -> str:
    return ",".join(f"{year}-{month:02d}" for year, month in sorted(months)) or "-"


# --- what the products read ---------------------------------------------------
#
# The `trips` view is the only thing the products read. Its columns come from the integrated
# table, except weather_category (derived from coco) and the optional columns: an optional
# column the source does not have yet is projected as NULL, so a product can declare a column
# that a future release will bring (humidity and aqi arrived in the Week 3 update) and keeps
# the same schema before and after it appears.

OPTIONAL_COLUMN_TYPES = {"humidity": "DOUBLE", "aqi": "DOUBLE"}


def report_projection(source_columns, columns) -> list:
    select = []
    for column in sorted(columns):
        if column == "weather_category":
            select.append(f"{WEATHER_CATEGORY_SQL} AS weather_category")
        elif column in source_columns:
            select.append(column)
        elif column in OPTIONAL_COLUMN_TYPES:
            select.append(f"CAST(NULL AS {OPTIONAL_COLUMN_TYPES[column]}) AS {column}")
        else:
            raise ValueError(f"required column {column} is missing from {SOURCE_TABLE}")
    return select


def register_report_view(spark: SparkSession, months, names, use_cache: bool = True) -> None:
    """Register `trips`: the slice of the integrated table (these months, these columns) the products read."""
    source_columns = set(spark.table(SOURCE_TABLE).columns)
    columns = {"year", "month"}
    for name in names:
        spec = PRODUCTS[name]
        columns |= set(spec["reads"]) | set(spec.get("optional", []))
        if "coco" in spec["reads"]:
            columns.add("weather_category")
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW trips AS
        SELECT {", ".join(report_projection(source_columns, columns))}
        FROM {SOURCE_TABLE}
        WHERE {partition_predicate(months)}
    """)

    # Two deliberate choices here, both measured in Week 2:
    #  1. the view selects only the columns the products need (at most 17 of the integrated
    #     table's 40), which means less to read and far less to keep in memory;
    #  2. CACHE TABLE materializes that slice once. The products aggregate the same rows, so
    #     without it Spark re-scans the integrated table once per product.
    if use_cache:
        spark.sql("CACHE TABLE trips")


def release_report_view(spark: SparkSession) -> None:
    spark.sql("UNCACHE TABLE IF EXISTS trips")


# --- the five data products -------------------------------------------------
#
# Design rule used by all of them: store ADDITIVE measures (trip_count, total_*, sum_*)
# next to the convenient averages. An average cannot be re-aggregated correctly
# (the mean of per-zone means is not the citywide mean), but a sum can, so storing
# the sums is what lets an analyst roll a product up to a coarser grain and still
# get the right number. In Week 3 the same rule is what makes the monthly grain possible:
# the all-months figures are recomputed from the monthly sums.


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
               -- Week 3: humidity is a new weather column; NULL until the source has it
               ROUND(AVG(humidity), 1)                   AS avg_humidity,
               year, month
        FROM trips
        GROUP BY service_date, day_of_week, year, month
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
        -- re-run a window function over the whole city every time it draws the table.
        -- Both are within one month, which is why this product refreshes month by month.
        SELECT *,
               RANK() OVER (PARTITION BY year, month ORDER BY trip_count DESC) AS demand_rank,
               ROUND(100.0 * trip_count / SUM(trip_count) OVER (PARTITION BY year, month), 3)
                   AS pct_of_monthly_trips
        FROM zone_month
    """)


def build_weather_impact_summary(spark: SparkSession) -> DataFrame:
    """One row per (month, weather category, pickup zone).

    Week 2 aggregated over all months at once, so one new month forced a rebuild from every
    trip. The monthly grain keeps sum_distance and sum_sq_distance unrounded, which is enough
    to recompute the all-months average and standard deviation exactly: the Week 2 shape is
    the weather_impact_summary view (WEATHER_IMPACT_V1_SQL).
    """
    return spark.sql("""
        SELECT year, month, weather_category, pickup_zone, pickup_borough,
               COUNT(*)                          AS trip_count,
               ROUND(SUM(trip_distance), 2)      AS total_distance_miles,
               ROUND(SUM(total_amount), 2)       AS total_revenue,
               ROUND(AVG(trip_distance), 3)      AS avg_trip_distance,
               ROUND(STDDEV(trip_distance), 3)   AS stddev_trip_distance,
               SUM(trip_distance)                AS sum_distance,
               SUM(trip_distance * trip_distance) AS sum_sq_distance,
               SUM(total_amount)                 AS sum_revenue
        FROM trips
        WHERE weather_category <> 'Unknown' AND pickup_zone IS NOT NULL
        GROUP BY year, month, weather_category, pickup_zone, pickup_borough
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
               -- Week 3: the AQI reported with the air-quality release; NULL until the source has it
               ROUND(AVG(aqi), 1)           AS avg_aqi,
               year, month
        FROM trips
        WHERE pm25 IS NOT NULL
        GROUP BY pickup_hour, year, month
    """)


def build_borough_mobility_summary(spark: SparkSession) -> DataFrame:
    """One row per (month, borough, weekday, hour); the Week 2 weekly profile is the view."""
    return spark.sql("""
        SELECT year, month, pickup_borough,
               date_format(tpep_pickup_datetime, 'EEEE') AS day_of_week,
               dayofweek(tpep_pickup_datetime)           AS day_of_week_num,
               hour(tpep_pickup_datetime)                AS hour_of_day,
               COUNT(*)                                  AS trip_count,
               ROUND(SUM(trip_distance), 2)              AS total_distance_miles,
               ROUND(SUM(total_amount), 2)               AS total_revenue,
               ROUND(AVG(trip_distance), 3)              AS avg_trip_distance,
               SUM(trip_distance)                        AS sum_distance,
               SUM(total_amount)                         AS sum_revenue
        FROM trips
        WHERE pickup_borough IS NOT NULL
        GROUP BY year, month, pickup_borough, day_of_week, day_of_week_num, hour_of_day
    """)


# --- compatibility views: the Week 2 shape of the two re-grained products ----------
#
# Queries written against Week 2 (show_products_in_use below, the dashboards) keep working
# unchanged: the view has the Week 2 name, columns and grain, and is recomputed from the
# monthly sums. The stored monthly table is available as <name>_monthly.

METADATA_ROLLUP = """MAX(source_table) AS source_table, MAX(source_version) AS source_version,
               MAX(schema_version) AS schema_version, MIN(created_at) AS created_at,
               MAX(refreshed_at) AS refreshed_at"""

WEATHER_IMPACT_V1_SQL = f"""
    SELECT weather_category, pickup_zone, pickup_borough,
           SUM(trip_count)                                  AS trip_count,
           ROUND(SUM(sum_distance), 2)                      AS total_distance_miles,
           ROUND(SUM(sum_revenue), 2)                       AS total_revenue,
           ROUND(SUM(sum_distance) / SUM(trip_count), 3)    AS avg_trip_distance,
           -- sample standard deviation from n, sum(x) and sum(x^2); NULL for one trip, like STDDEV
           ROUND(CASE WHEN SUM(trip_count) > 1 THEN
                 SQRT(GREATEST(SUM(sum_sq_distance) - POW(SUM(sum_distance), 2) / SUM(trip_count), 0)
                      / (SUM(trip_count) - 1)) END, 3)      AS stddev_trip_distance,
           {METADATA_ROLLUP}
    FROM weather_impact_summary_monthly
    GROUP BY weather_category, pickup_zone, pickup_borough
"""

BOROUGH_MOBILITY_V1_SQL = f"""
    SELECT pickup_borough, day_of_week, day_of_week_num, hour_of_day,
           SUM(trip_count)                               AS trip_count,
           ROUND(SUM(sum_distance), 2)                   AS total_distance_miles,
           ROUND(SUM(sum_revenue), 2)                    AS total_revenue,
           ROUND(SUM(sum_distance) / SUM(trip_count), 3) AS avg_trip_distance,
           {METADATA_ROLLUP}
    FROM borough_mobility_summary_monthly
    GROUP BY pickup_borough, day_of_week, day_of_week_num, hour_of_day
"""


# name -> how to build it and what it reads.
#   version:  bumped whenever the product's definition or output schema changes; a product
#             whose stored version differs is rebuilt in full by the refresh planner
#   reads:    integrated-table columns the product cannot be built without
#   optional: columns it uses when present (projected as NULL when the source lacks them)
#   compat:   the Week 2 view, for the products whose grain changed in Week 3
# All five are partitioned by (year, month): that is the unit of incremental refresh.
PRODUCTS = {
    "daily_mobility_summary": {
        "build": build_daily_mobility_summary,
        "version": "1.1",   # 1.1: avg_humidity
        "reads": ["tpep_pickup_datetime", "trip_distance", "total_amount", "passenger_count",
                  "temp", "prcp", "pm25"],
        "optional": ["humidity"],
    },
    "taxi_zone_statistics": {
        "build": build_taxi_zone_statistics,
        "version": "1.1",   # 1.1: partitioned by month
        "reads": ["pickup_zone", "pickup_borough", "trip_distance", "total_amount", "fare_amount"],
    },
    "weather_impact_summary": {
        "build": build_weather_impact_summary,
        "version": "2.0",   # 2.0: monthly grain (Week 2 shape served by the view)
        "reads": ["coco", "pickup_zone", "pickup_borough", "trip_distance", "total_amount"],
        "compat": WEATHER_IMPACT_V1_SQL,
    },
    "air_quality_impact_summary": {
        "build": build_air_quality_impact_summary,
        "version": "1.1",   # 1.1: partitioned by month, avg_aqi
        "reads": ["pickup_hour", "pm25", "trip_distance"],
        "optional": ["aqi"],
    },
    "borough_mobility_summary": {
        "build": build_borough_mobility_summary,
        "version": "2.0",   # 2.0: monthly grain (Week 2 shape served by the view)
        "reads": ["pickup_borough", "tpep_pickup_datetime", "trip_distance", "total_amount"],
        "compat": BOROUGH_MOBILITY_V1_SQL,
    },
}


def register_product_views(spark: SparkSession) -> None:
    """Expose every product under its Week 2 name; re-grained ones also as <name>_monthly."""
    for name, spec in PRODUCTS.items():
        table = spark.read.format("delta").load(product_path(name))
        if spec.get("compat"):
            table.createOrReplaceTempView(f"{name}_monthly")
            spark.sql(spec["compat"]).createOrReplaceTempView(name)
        else:
            table.createOrReplaceTempView(name)


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


def product_exists(name: str) -> bool:
    return os.path.isdir(os.path.join(product_path(name), "_delta_log"))


def product_created_at(spark: SparkSession, path: str, default: datetime) -> datetime:
    """When this product was first created: the timestamp of commit 0 in its own Delta log.

    Creation time is a property of the table, not of this run, so it is read back from the
    table rather than recomputed. On the very first build the table does not exist yet, and
    then this refresh IS the creation.
    """
    if not os.path.isdir(os.path.join(path, "_delta_log")):
        return default
    return spark.sql(f"DESCRIBE DETAIL {delta_sql_name(path)}").first()["createdAt"]


def source_columns_read(spark: SparkSession, name: str) -> dict:
    """name -> type of every source column this product reads, as recorded in the registry."""
    spec = PRODUCTS[name]
    types = {f.name: f.dataType.simpleString() for f in spark.table(SOURCE_TABLE).schema.fields}
    return {c: types[c] for c in sorted(set(spec["reads"]) | set(spec.get("optional", []))) if c in types}


def add_metadata_columns(df: DataFrame, source_version: int, schema_version: str,
                         created_at: datetime, refreshed_at: datetime) -> DataFrame:
    """Stamp every row with where it came from, when the table was created and when it was refreshed.

    These five constant columns are exactly what Week 2, Task 4 asks a data product to carry: the
    data source (and the source version it was built from), the schema version, the creation time
    and the refresh time. Constant columns look wasteful but cost almost nothing on disk (Parquet
    dictionary-encodes a repeated value), and they travel with the data: an analyst who exports
    the table to a spreadsheet still knows how old it is and which source version it reflects.
    After an incremental refresh they differ per month, which is exactly the truth.
    """
    return (df
            .withColumn("source_table", lit(SOURCE_TABLE))
            .withColumn("source_version", lit(source_version))
            .withColumn("schema_version", lit(schema_version))
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
    StructField("refresh_mode", StringType(), False),      # full | incremental
    # Week 3: what the refresh covered and why, and the source schema it was built from
    StructField("refreshed_partitions", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("source_columns", StringType(), True),     # JSON: column -> type
])


def append_registry_row(spark: SparkSession, row: dict) -> None:
    """Append one row to the registry table, creating it if it does not exist yet."""
    values = [tuple(row.get(field.name) for field in REGISTRY_SCHEMA.fields)]
    # mergeSchema: a Week 2 registry does not have the Week 3 columns yet
    spark.createDataFrame(values, REGISTRY_SCHEMA) \
        .write.format("delta").mode("append").option("mergeSchema", "true").save(REGISTRY_PATH)


#  writing
def write_product(spark: SparkSession, name: str, df: DataFrame, source_version: int,
                  months: list = None, reason: str = None) -> dict:
    """Materialize one product as a Delta table and return its registry row.

    months=None rewrites the whole table. Otherwise only those (year, month) partitions are
    replaced: df must hold exactly their rows (none, for a month that left the window).
    """
    spec = PRODUCTS[name]
    path = product_path(name)
    refreshed_at = datetime.now()
    created_at = product_created_at(spark, path, refreshed_at)
    start = time.time()

    stamped = add_metadata_columns(df, source_version, spec["version"], created_at, refreshed_at)

    # coalesce(1): the products are small, and spark.sql.shuffle.partitions=64 would
    # otherwise scatter a few hundred rows over 64 files. One file per month partition is
    # both smaller on disk and faster to open.
    writer = stamped.coalesce(1).write.format("delta").mode("overwrite").partitionBy(*PARTITION_COLS)

    # the same metadata, attached to the Delta commit itself, so `DESCRIBE HISTORY`
    # on the product explains where each version came from without the registry
    writer = writer.option("userMetadata", json.dumps({
        "product": name,
        "source_table": SOURCE_TABLE,
        "source_version": source_version,
        "schema_version": spec["version"],
        "refreshed_at": refreshed_at.isoformat(timespec="seconds"),
        "partitions": format_months(months) if months is not None else "all",
        "reason": reason,
    }))

    if months is not None:
        # incremental refresh: overwrite only the partitions matching this predicate and
        # leave every other month untouched (overwriteSchema is not allowed with it)
        writer = writer.option("replaceWhere", partition_predicate(months))
    else:
        writer = writer.option("overwriteSchema", "true")

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
        "schema_version": spec["version"],
        "created_at": created_at,
        "refreshed_at": refreshed_at,
        "row_count": row_count,
        "size_bytes": int(detail["sizeInBytes"]),
        "num_files": int(detail["numFiles"]),
        "build_seconds": build_seconds,
        "partitioned_by": ",".join(PARTITION_COLS),
        "refresh_mode": "incremental" if months is not None else "full",
        "refreshed_partitions": format_months(months) if months is not None else "all",
        "reason": reason,
        "source_columns": json.dumps(source_columns_read(spark, name)),
    }


def refresh(spark: SparkSession, names: list, source_version: int, window: list,
            months: list = None, reason: str = None, use_cache: bool = True,
            verbose: bool = True) -> list:
    # rebuild these products over the whole window (months=None) or over the given months only.

    in_window = window if months is None else [m for m in months if m in set(window)]
    register_report_view(spark, in_window, names, use_cache)
    rows = []
    try:
        for name in names:
            if verbose:
                print(f"\n--- building {name} ({format_months(months) if months is not None else 'full'}) ---")
            df = PRODUCTS[name]["build"](spark)
            row = write_product(spark, name, df, source_version, months, reason)
            append_registry_row(spark, row)
            if verbose:
                print(json.dumps({k: str(v) for k, v in row.items()}, indent=4))
            rows.append(row)
    finally:
        release_report_view(spark)
    return rows


# --- demonstration: the products answering the Week 2 queries ---------------

def show_products_in_use(spark: SparkSession) -> None:
    """Re-answer three of the Task 1 analyses straight off the products, to show they are enough."""
    # the Week 2 names: weather_impact_summary and borough_mobility_summary are the compatibility
    # views, so the SQL below is unchanged since Week 2. Reading the monthly tables instead would
    # make query 4 compute each zone's variation over (category x month) rows, which is wrong.
    register_product_views(spark)

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
        SELECT product_name, source_version, schema_version, refresh_mode, refreshed_partitions,
               row_count, size_bytes, num_files, build_seconds, refreshed_at
        FROM latest WHERE rn = 1
        ORDER BY product_name
    """).show(truncate=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild the analytical data products in full (Week 2, Task 4). "
                                                 "Use refresh_manager.py to refresh only what changed.")
    parser.add_argument("--product", default="all", choices=["all"] + list(PRODUCTS),
                        help="which product to rebuild (default: all)")
    parser.add_argument("--month", metavar="YYYY-MM",
                        help="rebuild only this month of the product(s)")
    parser.add_argument("--no-cache", action="store_true",
                        help="skip CACHE TABLE, to measure what caching is worth")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    from data_ingestion import build_spark
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    register_views(spark)

    window = discover_window(spark)
    names = list(PRODUCTS) if args.product == "all" else [args.product]
    months = None
    if args.month:
        year, month = (int(part) for part in args.month.split("-"))
        if (year, month) not in window:
            raise SystemExit(f"--month must be one of {format_months(window)}")
        months = [(year, month)]

    source_version = source_delta_version(spark)
    print(f"source: {SOURCE_TABLE} (Delta version {source_version}), window {format_months(window)}")

    refresh(spark, names, source_version, window, months,
            reason="requested on the command line", use_cache=not args.no_cache)

    if months is None and args.product == "all":
        show_products_in_use(spark)
    show_registry(spark)

    spark.stop()


if __name__ == "__main__":
    main()
