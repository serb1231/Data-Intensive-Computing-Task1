import os

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-memory 4g "
    "--packages io.delta:delta-spark_2.12:3.1.0 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession, DataFrame

INTEGRATED_PATH = "output_data/integrated_taxi_trips"
TRIP_DATA_PATH = "output_data/trip_data"
TAXI_ZONES_PATH = "output_data/taxi_zones"

# Meteostat `coco` condition code -> broad category. Defined once here and reused
# by every query that needs a human-readable weather bucket (see "Week2 Design
# Report.md", Task 1) instead of repeating the CASE WHEN in each query.
WEATHER_CATEGORY_SQL = """CASE WHEN coco IN (1, 2, 3) THEN 'Clear/Fair'
         WHEN coco IN (4, 5) THEN 'Cloudy/Overcast'
         WHEN coco IN (6, 7) THEN 'Fog'
         WHEN coco IN (8, 9, 17, 18) THEN 'Rain'
         WHEN coco IN (14, 15, 16, 19, 20) THEN 'Snow'
         WHEN coco IN (21, 22, 23, 24, 25, 26, 27) THEN 'Storm'
         ELSE 'Unknown' END"""

# guards query 4 against zones with so few trips that their variation across
# weather categories is noise rather than signal
MIN_ZONE_TRIPS = 500


def build_spark() -> SparkSession:
    # same local settings as data_ingestion.py / benchmark.py, so query timings
    # measured here reflect how the platform actually runs
    spark = SparkSession.builder.appName("Analytical_Query_Library") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def register_views(spark: SparkSession) -> None:
    """Register the Week 1 Delta tables as temp views so every query below is plain Spark SQL."""
    spark.read.format("delta").load(INTEGRATED_PATH).createOrReplaceTempView("integrated_taxi_trips")
    spark.read.format("delta").load(TRIP_DATA_PATH).createOrReplaceTempView("trip_data")
    spark.read.format("delta").load(TAXI_ZONES_PATH).createOrReplaceTempView("taxi_zones")


# --- 1. Monthly taxi demand for each taxi zone -----------------------------

def q1_monthly_demand_per_zone(spark: SparkSession) -> DataFrame:
    return spark.sql("""
        SELECT year, month, pickup_zone, pickup_borough,
               COUNT(*) AS trip_count,
               AVG(fare_amount) AS avg_fare
        FROM integrated_taxi_trips
        GROUP BY year, month, pickup_zone, pickup_borough
        ORDER BY year, month, trip_count DESC
    """)


def q1_monthly_demand_per_zone_underlying(spark: SparkSession) -> DataFrame:
    """Same analysis computed from trip_data + taxi_zones instead of the integrated table."""
    return spark.sql("""
        SELECT t.year, t.month, z.zone AS pickup_zone, z.borough AS pickup_borough,
               COUNT(*) AS trip_count,
               AVG(t.fare_amount) AS avg_fare
        FROM trip_data t
        LEFT JOIN taxi_zones z ON t.pulocationid = z.locationid
        GROUP BY t.year, t.month, z.zone, z.borough
        ORDER BY t.year, t.month, trip_count DESC
    """)


# --- 2. Average trip distance under different weather conditions -----------

def q2_avg_distance_by_weather(spark: SparkSession) -> DataFrame:
    return spark.sql(f"""
        SELECT weather_category,
               COUNT(*) AS trip_count,
               AVG(trip_distance) AS avg_distance,
               STDDEV(trip_distance) AS stddev_distance
        FROM (
            SELECT trip_distance, {WEATHER_CATEGORY_SQL} AS weather_category
            FROM integrated_taxi_trips
            WHERE coco IS NOT NULL
        )
        GROUP BY weather_category
        ORDER BY avg_distance DESC
    """)


# --- 3. Relationship between air quality and taxi demand -------------------

def q3_air_quality_vs_demand(spark: SparkSession) -> DataFrame:
    """Hourly trip demand alongside the citywide PM2.5 reading for that hour."""
    return spark.sql("""
        SELECT pickup_hour, pm25, COUNT(*) AS trip_count
        FROM integrated_taxi_trips
        WHERE pm25 IS NOT NULL
        GROUP BY pickup_hour, pm25
        ORDER BY pickup_hour
    """)


def q3_pm25_demand_correlation(spark: SparkSession) -> float:
    """Single Pearson correlation coefficient summarizing q3_air_quality_vs_demand."""
    q3_air_quality_vs_demand(spark).createOrReplaceTempView("hourly_demand_pm25")
    row = spark.sql("SELECT corr(pm25, trip_count) AS pm25_demand_correlation FROM hourly_demand_pm25").first()
    return row["pm25_demand_correlation"]


def q3_demand_by_aqi_band(spark: SparkSession) -> DataFrame:
    """Readable companion to q3_air_quality_vs_demand, bucketed into EPA AQI bands for PM2.5."""
    return spark.sql("""
        SELECT CASE WHEN pm25 < 12 THEN 'Good'
                    WHEN pm25 < 35.4 THEN 'Moderate'
                    ELSE 'Unhealthy' END AS aqi_band,
               COUNT(*) AS trip_count,
               AVG(trip_distance) AS avg_distance
        FROM integrated_taxi_trips
        WHERE pm25 IS NOT NULL
        GROUP BY aqi_band
        ORDER BY trip_count DESC
    """)


# --- 4. Taxi zones with the largest variation in demand under weather ------

def q4_zone_weather_variation(spark: SparkSession) -> DataFrame:
    return spark.sql(f"""
        WITH zone_weather_categories AS (
            SELECT pickup_zone, {WEATHER_CATEGORY_SQL} AS weather_category
            FROM integrated_taxi_trips
            WHERE coco IS NOT NULL AND pickup_zone IS NOT NULL
        ),
        counted AS (
            SELECT pickup_zone, weather_category, COUNT(*) AS trip_count
            FROM zone_weather_categories
            GROUP BY pickup_zone, weather_category
        )
        SELECT pickup_zone,
               SUM(trip_count) AS total_trips,
               STDDEV(trip_count) AS demand_stddev,
               STDDEV(trip_count) / AVG(trip_count) AS coeff_of_variation
        FROM counted
        GROUP BY pickup_zone
        HAVING SUM(trip_count) >= {MIN_ZONE_TRIPS} AND COUNT(*) >= 2
        ORDER BY coeff_of_variation DESC
    """)


# --- 5. Peak travel hours for each day of the week --------------------------

def q5_peak_hour_per_weekday(spark: SparkSession) -> DataFrame:
    return spark.sql("""
        WITH hourly_counts AS (
            SELECT date_format(tpep_pickup_datetime, 'EEEE') AS day_of_week,
                   hour(tpep_pickup_datetime) AS hour_of_day,
                   COUNT(*) AS trip_count
            FROM integrated_taxi_trips
            GROUP BY day_of_week, hour_of_day
        ),
        ranked AS (
            SELECT *, RANK() OVER (PARTITION BY day_of_week ORDER BY trip_count DESC) AS rnk
            FROM hourly_counts
        )
        SELECT day_of_week, hour_of_day, trip_count
        FROM ranked
        WHERE rnk = 1
        ORDER BY CASE day_of_week
            WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
            WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6
            ELSE 7 END
    """)


def q5_hourly_counts_per_weekday(spark: SparkSession) -> DataFrame:
    """Full (day, hour) breakdown behind q5_peak_hour_per_weekday, e.g. for a heatmap."""
    return spark.sql("""
        SELECT date_format(tpep_pickup_datetime, 'EEEE') AS day_of_week,
               hour(tpep_pickup_datetime) AS hour_of_day,
               COUNT(*) AS trip_count
        FROM integrated_taxi_trips
        GROUP BY day_of_week, hour_of_day
        ORDER BY day_of_week, hour_of_day
    """)


# --- 6. Monthly trends in taxi demand ---------------------------------------

def q6_monthly_demand_trend(spark: SparkSession) -> DataFrame:
    return spark.sql("""
        WITH monthly AS (
            SELECT year, month, COUNT(*) AS trip_count, SUM(total_amount) AS revenue
            FROM integrated_taxi_trips
            GROUP BY year, month
        )
        SELECT year, month, trip_count, revenue,
               trip_count - LAG(trip_count) OVER (ORDER BY year, month) AS mom_change,
               ROUND(100.0 * (trip_count - LAG(trip_count) OVER (ORDER BY year, month))
                     / LAG(trip_count) OVER (ORDER BY year, month), 2) AS mom_pct_change
        FROM monthly
        ORDER BY year, month
    """)


def q6_monthly_demand_trend_underlying(spark: SparkSession) -> DataFrame:
    """Same analysis computed from trip_data alone (no enrichment columns needed)."""
    return spark.sql("""
        WITH monthly AS (
            SELECT year, month, COUNT(*) AS trip_count, SUM(total_amount) AS revenue
            FROM trip_data
            GROUP BY year, month
        )
        SELECT year, month, trip_count, revenue,
               trip_count - LAG(trip_count) OVER (ORDER BY year, month) AS mom_change,
               ROUND(100.0 * (trip_count - LAG(trip_count) OVER (ORDER BY year, month))
                     / LAG(trip_count) OVER (ORDER BY year, month), 2) AS mom_pct_change
        FROM monthly
        ORDER BY year, month
    """)


# the six analyses required by Task 1/2, each answerable straight off the
# integrated table -- this is the "analytical query library" referenced in Task 4
QUERIES = {
    "1_monthly_demand_per_zone": q1_monthly_demand_per_zone,
    "2_avg_distance_by_weather": q2_avg_distance_by_weather,
    "3_air_quality_vs_demand": q3_air_quality_vs_demand,
    "4_zone_weather_variation": q4_zone_weather_variation,
    "5_peak_hour_per_weekday": q5_peak_hour_per_weekday,
    "6_monthly_demand_trend": q6_monthly_demand_trend,
}

# same analyses answered directly off the underlying Week 1 tables instead of the
# integrated table, kept for the Task 3 broadcast-join / partition-pruning experiments
UNDERLYING_QUERIES = {
    "1_monthly_demand_per_zone": q1_monthly_demand_per_zone_underlying,
    "6_monthly_demand_trend": q6_monthly_demand_trend_underlying,
}


def run_all_queries(spark: SparkSession) -> dict:
    return {name: query_func(spark) for name, query_func in QUERIES.items()}


def main() -> None:
    spark = build_spark()
    register_views(spark)

    for name, query_func in QUERIES.items():
        print(f"\n=== Query {name} ===")
        query_func(spark).show(10, truncate=False)

    print("\n=== Query 3b: PM2.5 vs. hourly demand correlation ===")
    print(f"correlation coefficient: {q3_pm25_demand_correlation(spark):.4f}")

    print("\n=== Query 3c: demand by AQI band ===")
    q3_demand_by_aqi_band(spark).show(truncate=False)

    print("\n=== Query 5b: full day x hour breakdown ===")
    q5_hourly_counts_per_weekday(spark).show(20, truncate=False)

    print("\n=== Underlying-table variants (same analyses off trip_data/taxi_zones) ===")
    for name, query_func in UNDERLYING_QUERIES.items():
        print(f"\n--- {name} (underlying tables) ---")
        query_func(spark).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
