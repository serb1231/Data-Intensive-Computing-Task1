import time

from analytical_queries import build_spark, register_views, q1_monthly_demand_per_zone_underlying, q2_avg_distance_by_weather, WEATHER_CATEGORY_SQL

aqe_test_query = """
        SELECT t.year, z.borough, COUNT(*) AS expensive_trips
        FROM trip_data t
        JOIN taxi_zones z ON t.pulocationid = z.locationid
        WHERE t.fare_amount > 100 
        GROUP BY t.year, z.borough
    """


def time_and_explain(spark, query_name, df, description):
    print(f"\n==================================================")
    print(f"--- {query_name}: {description} ---")

    start_time = time.time()
    # use collect as the show will only return the first 20 entries, hence hidden optimization
    result_data = df.collect()
    end_time = time.time()

    print(f"Execution Time: {round(end_time - start_time, 2)} seconds")
    print("Physical Plan:")
    df.explain("formatted")
    print(f"==================================================\n")

    return result_data

def verify_optimization(baseline_data, optimized_data):
    assert(baseline_data == optimized_data)

def main():
    spark = build_spark()
    register_views(spark)

    # CACHING
    # Baseline: Execute the same query twice from disk
    spark.catalog.clearCache()
    time_and_explain(spark, "Caching Test", q1_monthly_demand_per_zone_underlying(spark), "Read from Disk (First Run)")
    b1 = time_and_explain(spark, "Caching Test", q1_monthly_demand_per_zone_underlying(spark), "Read from Disk (Second Run)")

    # Optimized: Cache the tables into RAM first
    spark.catalog.cacheTable("trip_data")
    spark.catalog.cacheTable("taxi_zones")
    time_and_explain(spark, "Caching Test", q1_monthly_demand_per_zone_underlying(spark),
                     "Read from RAM Cache (During Lazy option by spark")
    o1 = time_and_explain(spark, "Caching Test", q1_monthly_demand_per_zone_underlying(spark),
                     "Read from RAM Cache (Optimized truly)")



    # Partition Pruning
    # Baseline: Don't use a partitioned column. Use something that will make spark bring into RAM all the data
    df_prune_baseline = spark.sql("""
        SELECT COUNT(*) FROM trip_data WHERE tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2024-02-01'
    """)
    b2 = time_and_explain(spark, "Partition Pruning", df_prune_baseline, "Full Scan (Baseline)")

    # Optimized: Use the partition by referencing a partitioned column (year and month)
    df_prune_optimized = spark.sql("""
        SELECT COUNT(*) FROM trip_data WHERE year = 2024 AND month = 1
    """)
    o2 = time_and_explain(spark, "Partition Pruning", df_prune_optimized, "Partition Pruned (Optimized)")

    # BROADCAST JOIN
    # Baseline: Normal Join
    print("Warming up the OS cache to ensure a fair Broadcast test...")
    q1_monthly_demand_per_zone_underlying(spark).collect()

    # Baseline: Normal Join
    # deactivate the automatic broadcast join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    df_join_baseline = q1_monthly_demand_per_zone_underlying(spark)
    b3 = time_and_explain(spark, "Broadcast Test", df_join_baseline, "Standard SortMergeJoin (Baseline)")

    # Optimized: Add a SQL Hint to force a Broadcast Join
    # turn broadcast join back on
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    # we need to redefine it in order to not cache the result
    df_join_optimized = spark.sql("""
        SELECT t.year, t.month, z.zone AS pickup_zone, z.borough AS pickup_borough,
               COUNT(*) AS trip_count,
               AVG(t.fare_amount) AS avg_fare
        FROM trip_data t
        LEFT JOIN taxi_zones z ON t.pulocationid = z.locationid
        GROUP BY t.year, t.month, z.zone, z.borough
        ORDER BY t.year, t.month, trip_count DESC
    """)
    o3 = time_and_explain(spark, "Broadcast Test", df_join_optimized, "BroadcastHashJoin (Optimized)")


    # AQE
    # AQE disabled
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    df_aqe_off = spark.sql(aqe_test_query)
    b4 = time_and_explain(spark, "AQE Test", df_aqe_off, "AQE Disabled (Baseline)")

    # AQE enabled
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    df_aqe_on = spark.sql(aqe_test_query)
    o4 = time_and_explain(spark, "AQE Test", df_aqe_on, "AQE Enabled (Optimized)")

    verify_optimization(b1, o1)
    verify_optimization(b2, o2)
    verify_optimization(b3, o3)
    verify_optimization(b4, o4)

    spark.stop()


if __name__ == "__main__":
    main()