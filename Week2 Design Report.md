# Week 2 Design Report

## Task 1. Design Analytical Queries

### Reference: schema and physical layout in play

All six analyses read from `integrated_taxi_trips` (Delta, partitioned by `year`, `month`), which already carries one row per trip enriched with:

- **Trip facts**: `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `trip_distance`, `fare_amount`, `total_amount`, `passenger_count`, `pulocationid`, `dolocationid`, `year`, `month`
- **Weather at pickup hour**: `temp`, `rhum`, `prcp`, `snwd`, `wspd`, `pres`, `coco` (Meteostat weather-condition code)
- **Air quality at pickup hour**: `pm25` (µg/m³, NYC citywide hourly average)
- **Zone/borough context**: `pickup_zone`, `pickup_borough`, `dropoff_zone`, `dropoff_borough`

Because weather and air quality are already denormalized onto every trip, none of the six analyses below need a runtime join against `weather`, `air_quality`, or `taxi_zones` — that join cost was paid once, at integration time. This is the key design fact that shapes Task 3 (optimization): the interesting work is not "make the join cheap" but "avoid scanning trip-level columns/partitions the query doesn't need," and for the two demand queries that do touch the raw `trip_data`/`taxi_zones` tables directly, a broadcast join is the relevant technique. Each query below states its primary source and the alternative (underlying tables) where that alternative is a real design option.

For the two queries that bucket a continuous weather signal (distance-under-weather and zone-variation-under-weather), `coco` is mapped to broad categories via the standard Meteostat condition-code ranges, since raw codes (1–27) are too fine-grained to be human-readable in a report table:

| `coco` range | Category |
| --- | --- |
| 1–3 | Clear / Fair |
| 4–5 | Cloudy / Overcast |
| 6–7 | Fog |
| 8–9, 17–18 | Rain |
| 14–16, 19–20 | Snow |
| 21–27 | Storm |

This mapping is defined once (as a `CASE WHEN` expression or a small broadcast lookup) and reused by every query below that needs a weather category, so the bucketing logic isn't duplicated six times in Task 2.

---

### 1. Monthly taxi demand for each taxi zone

- **Business question**: How many trips originate in each pickup zone, per month? Which zones are growing/shrinking?
- **Grain of output**: one row per `(year, month, pickup_zone)`
- **Primary source**: `integrated_taxi_trips` — but since this query touches no weather/air-quality columns, it is also answerable directly from `trip_data` joined to `taxi_zones` (a ~265-row broadcast candidate). That alternative is deliberately kept in scope for Task 3, where it's compared against querying the integrated table.
- **Measures**: `COUNT(*) AS trip_count`, optionally `AVG(fare_amount)`, `AVG(trip_distance)`
- **Grouping**: `year`, `month`, `pickup_zone` (`pickup_borough` optionally included for roll-up)
- **Sketch**:
  ```sql
  SELECT year, month, pickup_zone, pickup_borough,
         COUNT(*) AS trip_count,
         AVG(fare_amount) AS avg_fare
  FROM integrated_taxi_trips
  GROUP BY year, month, pickup_zone, pickup_borough
  ORDER BY year, month, trip_count DESC
  ```
- **Design notes**: `year`/`month` are partition columns, so a query scoped to specific months prunes partitions directly. This is the cleanest partition-pruning candidate of the six.

### 2. Average trip distance under different weather conditions

- **Business question**: Do people travel farther when it rains, snows, or is clear?
- **Grain of output**: one row per weather category (optionally per month, to see if the effect is seasonal)
- **Primary source**: `integrated_taxi_trips` (weather columns already attached; no join needed)
- **Measures**: `AVG(trip_distance)`, `COUNT(*)`, `STDDEV(trip_distance)` (to show spread, not just mean)
- **Grouping**: derived `weather_category` (via the `coco` mapping above); `prcp > 0` / `snwd > 0` flags as a cross-check against the categorical mapping
- **Sketch**:
  ```sql
  SELECT weather_category,
         COUNT(*) AS trip_count,
         AVG(trip_distance) AS avg_distance,
         STDDEV(trip_distance) AS stddev_distance
  FROM (
    SELECT trip_distance,
           CASE WHEN coco IN (1,2,3) THEN 'Clear'
                WHEN coco IN (4,5) THEN 'Cloudy'
                WHEN coco IN (6,7) THEN 'Fog'
                WHEN coco IN (8,9,17,18) THEN 'Rain'
                WHEN coco IN (14,15,16,19,20) THEN 'Snow'
                WHEN coco IN (21,22,23,24,25,26,27) THEN 'Storm'
                ELSE 'Unknown' END AS weather_category
    FROM integrated_taxi_trips
    WHERE coco IS NOT NULL
  )
  GROUP BY weather_category
  ORDER BY avg_distance DESC
  ```
- **Design notes**: No partition pruning available (weather isn't a partition column) — this is a full-table scan by nature, making it a good candidate for caching if run repeatedly, which Task 3 will test.

### 3. Relationship between air quality and taxi demand

- **Business question**: Does taxi demand rise or fall as PM2.5 levels increase (e.g., people avoiding walking/transit on bad-air days)?
- **Grain of output**: one row per hour (`pickup_hour`), giving `(trip_count, avg_pm25)`; a second aggregate rolls this up into a single correlation coefficient
- **Primary source**: `integrated_taxi_trips`
- **Measures**: `COUNT(*) AS trip_count` per hour, `AVG(pm25)` per hour (already ~constant within an hour since `pm25` is itself an hourly citywide figure — the aggregation mainly exists to line demand up on the same `pickup_hour` grain), then `corr(trip_count, avg_pm25)` across hours; also an AQI-style bucketed view (`Good` &lt;12, `Moderate` 12–35.4, `Unhealthy` &gt;35.4 µg/m³) for a readable breakdown
- **Sketch**:
  ```sql
  WITH hourly AS (
    SELECT pickup_hour, pm25, COUNT(*) AS trip_count
    FROM integrated_taxi_trips
    WHERE pm25 IS NOT NULL
    GROUP BY pickup_hour, pm25
  )
  SELECT corr(pm25, trip_count) AS pm25_demand_correlation
  FROM hourly;

  -- bucketed view for the report
  SELECT CASE WHEN pm25 < 12 THEN 'Good'
              WHEN pm25 < 35.4 THEN 'Moderate'
              ELSE 'Unhealthy' END AS aqi_band,
         COUNT(*) AS trip_count,
         AVG(trip_distance) AS avg_distance
  FROM integrated_taxi_trips
  WHERE pm25 IS NOT NULL
  GROUP BY aqi_band
  ```
- **Design notes**: Same shape as query 2 — full scan, no partition pruning, good caching candidate if the hourly aggregate is reused between the correlation and the bucketed breakdown (materialize `hourly` once, reuse it — a direct target for the caching experiment in Task 3).

### 4. Taxi zones with the largest variation in demand under different weather conditions

- **Business question**: Which pickup zones are most weather-sensitive (e.g., outdoor/leisure zones swing a lot between clear and rainy days; transit hubs stay flat)? Useful for fleet repositioning under changing weather.
- **Grain of output**: one row per `pickup_zone`, with a variation statistic across weather categories
- **Primary source**: `integrated_taxi_trips`
- **Measures**: two-stage aggregation — (1) `trip_count` per `(pickup_zone, weather_category)`, (2) `STDDEV` (or `MAX - MIN`) of those counts per `pickup_zone`, normalized by the zone's average count so high-volume and low-volume zones are comparable
- **Sketch**:
  ```sql
  WITH zone_weather_counts AS (
    SELECT pickup_zone, weather_category, COUNT(*) AS trip_count
    FROM integrated_taxi_trips_with_weather_category   -- category derived as in query 2
    GROUP BY pickup_zone, weather_category
  )
  SELECT pickup_zone,
         STDDEV(trip_count) AS demand_stddev,
         STDDEV(trip_count) / AVG(trip_count) AS coeff_of_variation
  FROM zone_weather_counts
  GROUP BY pickup_zone
  ORDER BY coeff_of_variation DESC
  LIMIT 20
  ```
- **Design notes**: Heaviest query of the six — full scan plus a `(zone, weather_category)` shuffle, then a second aggregation. Zones with very few trips can produce misleadingly large coefficients of variation, so a `HAVING COUNT(*) > threshold` filter on the first-stage count is part of the design, not just a nice-to-have.

### 5. Peak travel hours for each day of the week

- **Business question**: For each day of the week, which hour has the most pickups (e.g., is Friday's peak at 17:00 or 23:00)?
- **Grain of output**: one row per `(day_of_week, hour_of_day)`, plus a derived "peak hour" row per day
- **Primary source**: `integrated_taxi_trips`, or `trip_data` directly — this query only needs `tpep_pickup_datetime`, so it's another candidate for comparing the integrated table against the narrower underlying table in Task 3
- **Measures**: `COUNT(*) AS trip_count`; `RANK()`/`ROW_NUMBER()` window function over `(day_of_week)` ordered by `trip_count DESC` to extract the single peak hour per day
- **Sketch**:
  ```sql
  WITH hourly_counts AS (
    SELECT date_format(tpep_pickup_datetime, 'EEEE') AS day_of_week,
           hour(tpep_pickup_datetime) AS hour_of_day,
           COUNT(*) AS trip_count
    FROM integrated_taxi_trips
    GROUP BY day_of_week, hour_of_day
  )
  SELECT day_of_week, hour_of_day, trip_count,
         RANK() OVER (PARTITION BY day_of_week ORDER BY trip_count DESC) AS rnk
  FROM hourly_counts
  QUALIFY rnk = 1
  ```
- **Design notes**: `year`/`month` partition pruning still applies if the report is scoped to a date range; the window function itself operates on a small post-aggregation result (7 days × 24 hours = 168 rows) so it's cheap once the base aggregation is done.

### 6. Monthly trends in taxi demand

- **Business question**: Is overall citywide demand growing, shrinking, or seasonal month over month?
- **Grain of output**: one row per `(year, month)`
- **Primary source**: `integrated_taxi_trips` (or `trip_data`, same reasoning as query 1)
- **Measures**: `COUNT(*) AS trip_count`, `SUM(total_amount) AS revenue`, plus a `LAG`-based month-over-month percentage change
- **Sketch**:
  ```sql
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
  ```
- **Design notes**: This is the query most directly aligned with the table's own partitioning (`year`, `month`) — a textbook partition-pruning case when scoped to a subset of months, and the natural query to demonstrate AQE's effect on the final small shuffle (168-row-scale window function over a low-cardinality key).

---

### Summary table

| # | Analysis | Grain | Primary source | Underlying-table alternative | Partition pruning? |
| --- | --- | --- | --- | --- | --- |
| 1 | Monthly demand per zone | year, month, zone | `integrated_taxi_trips` | `trip_data` ⋈ `taxi_zones` (broadcast) | Yes (`year`, `month`) |
| 2 | Avg distance by weather | weather category | `integrated_taxi_trips` | — (needs weather columns) | No |
| 3 | Air quality vs. demand | pickup hour | `integrated_taxi_trips` | — (needs `pm25`) | No |
| 4 | Zone variation by weather | zone | `integrated_taxi_trips` | — (needs weather columns) | No |
| 5 | Peak hours per weekday | day_of_week, hour | `integrated_taxi_trips` | `trip_data` | Partial (if date-scoped) |
| 6 | Monthly demand trend | year, month | `integrated_taxi_trips` | `trip_data` | Yes (`year`, `month`) |

This mapping is what Task 3's optimization experiments will exercise: queries 1, 5, and 6 are where partition pruning and broadcast joins against `taxi_zones` have something to prune/broadcast; queries 2, 3, and 4 are full-scan by nature and are the caching/AQE candidates instead.

---

## Task 4. Reusable Analytical Data Products

### What was built

Five Delta tables under `output_data/data_products/`, generated by `data_products.py` from the
integrated dataset. Each one is a pre-aggregated answer to one or more of the six analyses
designed in Task 1:

| Data product | Grain | Rows | Size | Serves |
| --- | --- | --- | --- | --- |
| `daily_mobility_summary` | `service_date` | 91 | 20 KB | Q6 (trend), daily reporting |
| `taxi_zone_statistics` | `year, month, pickup_zone` | 755 | 32 KB | Q1 |
| `weather_impact_summary` | `weather_category, pickup_zone` | 1,215 | 38 KB | Q2 and Q4 |
| `air_quality_impact_summary` | `pickup_hour` | 2,182 | 53 KB | Q3 |
| `borough_mobility_summary` | `pickup_borough, day_of_week, hour_of_day` | 1,168 | 28 KB | Q5 |

Together: **5,411 rows and 171 KB, against a source table of 8,480,540 rows and 577 MB** — the
products cost 0.03% of the storage of the table they summarize.

### Design decisions

**Additive measures.** Every product stores `trip_count` and `total_*` sums next to the
convenient averages. An average cannot be re-aggregated (the mean of per-zone means is not the
citywide mean), a sum can. This is what allows `weather_impact_summary` — stored at
`(weather_category, pickup_zone)` — to answer two different analyses from one table: rolled up
over zones with `SUM(total_distance_miles) / SUM(trip_count)` it is Q2, and grouped by zone with
`STDDEV(trip_count) / AVG(trip_count)` it is Q4. Storing the averages alone would have forced a
separate table per question.

**Scoping.** The trip files cover January–March 2024, but ~20 rows carry corrupt pickup
timestamps and land in partitions such as `year=2002` or `year=2024/month=4`. A report table
with a row for "2002-12-31" is worse than no row, so every product is scoped to the reporting
window. Because `year`/`month` are derived from the pickup timestamp *and* are the partition
columns, that filter is also a partition prune: Spark never opens those partitions. This follows
the same scoping-is-not-rejection principle used for the nationwide EPA extract in Week 1.

**One scan, five products.** All five products aggregate the same slice of the integrated table,
so `data_products.py` registers a narrowed view (14 of the 38 columns) and `CACHE TABLE`s it once.
Without the cache, Spark re-scans the source five times. After that single scan, the five
aggregations take 7.9 s combined (2.56 / 1.35 / 1.24 / 0.94 / 1.85 s).

**Metadata, in three places.**

1. *On every row*: `source_table`, `source_version` (the Delta version of the integrated table the
   product was built from), `schema_version`, `created_at` and `refreshed_at` — the four fields the
   task asks for, plus the source version. Creation time is read back from the product's own Delta
   log (the timestamp of commit 0 via `DESCRIBE DETAIL`) rather than recomputed, so it survives
   every refresh: after the products were rebuilt the following morning, `created_at` still read
   2026-09-19 19:15 while `refreshed_at` read 2026-09-20 10:23. Constant columns are almost free in
   Parquet (dictionary encoding) and they travel with the data when an analyst exports it.
2. *In a registry table* (`output_data/data_products/_registry`): append-only, one row per refresh,
   holding the same identity fields plus `row_count`, `size_bytes`, `num_files`, `build_seconds`,
   `partitioned_by` and `refresh_mode`. Because it is append-only it doubles as a refresh log, and
   Task 5 reads its storage and build-cost figures instead of re-measuring them.
3. *In the Delta commit*: each write passes `userMetadata`, so `DESCRIBE HISTORY` on a product
   explains every version without consulting the registry at all.

**Efficient querying.** The products are small, so the honest optimization is *not* to partition
them: `coalesce(1)` writes one file per table, because `spark.sql.shuffle.partitions=64` would
otherwise scatter a few hundred rows across 64 files and make reads slower, not faster.
Z-ORDER on `taxi_zone_statistics` would be pure ceremony at 755 rows. The one exception is
`daily_mobility_summary`, partitioned by `year`/`month` because it is the only product that grows
with time: `python data_products.py --month 2024-03` rebuilds just that month using Delta's
`replaceWhere`, leaving January and February untouched (verified: the table still holds 91 rows
afterwards, and the January rows keep their earlier `refreshed_at` stamp).

### Who uses each product, and why materialize it

| Product | Who | Why it is useful | Why materialized rather than computed on demand |
| --- | --- | --- | --- |
| `daily_mobility_summary` | City mobility operations, daily dashboard | One row per day: demand, revenue, and the weather and air the city moved in | Read every morning by many people; the months it summarizes are immutable history, so recomputing an 8.5M-row aggregation for an unchanged answer is pure waste |
| `taxi_zone_statistics` | TLC planners, fleet repositioning | Zone league table with rank and share of monthly trips, joinable to the zone map for a choropleth | The rank and share are window functions over the whole city; a BI tool re-fires them on every filter click, and materializing pays that cost once per month instead of once per click |
| `weather_impact_summary` | Demand forecasting, surge staffing | How far and how often people travel in each weather category, per zone | The underlying query is a full scan with no partition pruning available (`coco` is not a partition column) — the expensive shape — consumed as five rows |
| `air_quality_impact_summary` | Public-health and environment analysts (DOHMH) | Hourly PM2.5 next to hourly demand, with the EPA AQI band pre-computed | The cross-domain join was already paid at integration time; banding and correlation over 2,182 hourly rows is instant, over 8.5M trips it is not |
| `borough_mobility_summary` | Borough-level policy, shift planning | Weekly demand profile (weekday × hour) for each borough | Q5's ranking is cheap only *after* the base aggregation; materializing removes the heavy part from the analyst's path and keeps the borough breakdown available for free |

### Why these are worth materializing at all

Storage overhead is not the constraint. The five products together occupy 171,216 bytes against the
576,913,745-byte source — 0.03% — so the decision turns entirely on whether the latency saved
justifies running a refresh job. Measured on the integrated table versus its products, the saving
is ×2.4 to ×6.0 depending on the analysis, and the products return results identical to the Task 2
queries. Full figures, methodology and validation are in `Week2 Benchmark Report.md`.

Those speed-ups are smaller than the size ratio suggests, and the reason is a property of the
platform rather than a weakness of the products: columnar Parquet reads only the two or three
columns a query needs out of 38, and the `year`/`month` predicate prunes partitions before a file
is opened, so an ad-hoc query over three months is already sub-second. The design arguments that
carry the decision are therefore the ones independent of today's data size:

- **Cost scales with the source, not the report.** The ad-hoc query grows with the trip table; the
  product stays at a few thousand rows. At ten cities and several years the gap widens from ×3 to
  orders of magnitude, while storage overhead stays a fraction of a percent.
- **One definition of each number.** The `coco` weather-category mapping, the AQI bands, the
  500-trip threshold and the coefficient-of-variation formula live in one place instead of being
  retyped by every analyst.
- **Concurrency.** Twenty analysts on a dashboard run either twenty scans of the trip table or
  twenty reads of a 38 KB file; the single-user measurements understate this.

### Trade-offs accepted

- **Staleness.** A product is as old as its last refresh, which is why `refreshed_at` is on every
  row rather than buried in a log: an analyst can see the age of the number they are quoting.
- **A second thing to keep correct.** The products duplicate logic that also exists in the query
  library, so the shared definitions (`WEATHER_CATEGORY_SQL`, the scoping window) are imported
  rather than copied, and the validation in the benchmark report exists to catch drift.
- **A fixed reporting window.** The window is currently the three months the source covers; adding
  a fourth month means the constant moves, or the window is derived from the data itself.
