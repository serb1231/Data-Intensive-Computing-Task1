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
