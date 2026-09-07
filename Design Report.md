## Task 1. Study the Data

### TripData

- **What is the primary entity represented by the dataset?**
  Individual taxi trips.
- **Which attributes uniquely identify a record (i.e., what is the primary key of the dataset)?**
  `tpep_pickup_datetime` and `tpep_dropoff_datetime`. There are duplicates of these, all of which have a positive or negative sum (hinting at a reimbursement from the company). It may be necessary to create a surrogate key (a new key based on a hash).
- **Which attributes are likely to be used for joins?**
  `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, and `DOLocationID`. The time and date are used to join with the Weather and Taxi Zone Lookup datasets.
- **Which attributes are temporal?**
  `tpep_pickup_datetime` and `tpep_dropoff_datetime`.
- **Which attributes contain categorical values?**
  `VendorID`, `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `PULocationID`, `DOLocationID`, and `payment_type`.
- **Which attributes are likely to grow over time?**
  None; the database grows vertically, not horizontally.

---

### Weather

- **What is the primary entity represented by the dataset?**
  Hourly weather conditions (snow, wind, rain, temperature).
- **Which attributes uniquely identify a record (i.e., what is the primary key of the dataset)?**
  `year`, `month`, `day`, and `hour`.
- **Which attributes are likely to be used for joins?**
  `year`, `month`, `day`, and `hour`.
- **Which attributes are temporal?**
  `year`, `month`, `day`, and `hour`.
- **Which attributes contain categorical values?**
  `temp_source`, `rhum`, `rhum_source`, `prcp_source`, `snwd_source`, `wdir`, `wdir_source`, `wspd_source`, `wpgt_source`, `pres_source`, `cldc`, `cldc_source`, `coco`, and `coco_source`.
- **Which attributes are likely to grow over time?**
  None; the database grows vertically, not horizontally.

---

### Air Quality

- **What is the primary entity represented by the dataset?**
  Air quality based on location and time.
- **Which attributes uniquely identify a record (i.e., what is the primary key of the dataset)?**
  `State Code`, `County Code`, `Parameter Code`, `Site Num`, `Date GMT`, `Time GMT`, `Method Name`, and `POC`.
- **Which attributes are likely to be used for joins?**
  `State Code`, `County Code`, `Date GMT`, and `Time GMT`.
- **Which attributes are temporal?**
  `Date Local`, `Time Local`, `Date GMT`, and `Time GMT`.
- **Which attributes contain categorical values?**
  `State Code`, `County Code`, `Parameter Code`, `Site Num`, `POC`, `Datum`, `Parameter Name`, `Units of Measure`, `MDL`, `Method Type`, `Method Code`, `Method Name`, `State Name`, and `County Name`.
- **Which attributes are likely to grow over time?**
  None; the database grows vertically, not horizontally.

---

### Taxi Zone Lookup

- **What is the primary entity represented by the dataset?**
  Locations of taxi zones.
- **Which attributes uniquely identify a record (i.e., what is the primary key of the dataset)?**
  `LocationID`.
- **Which attributes are likely to be used for joins?**
  `Borough` and `Zone`.
- **Which attributes are temporal?**
  N/A
- **Which attributes contain categorical values?**
  `Borough`, `Zone`, and `service_zone`.
- **Which attributes are likely to grow over time?**
  N/A

## Task 2. Design Your Storage Architecture

### Storage Architecture Design

**1. Directory Structure**

- `data/`: Raw, unprocessed source files (CSV/Parquet), read directly by the ingestion framework.
- `output_data/`: Processed Delta tables written by `execute_pipeline`, one subdirectory per dataset.

**2. Delta Table Organization & Naming Conventions**

- _Naming Convention:_ All tables and columns must be lowercase, with words separated by underscores (`_`).
- `yellow_tripdata_2024-01`, `2024-02`, `2024-03` (unioned) $\rightarrow$ `trip_data`
- `taxi_zone_lookup.csv` $\rightarrow$ `taxi_zones`
- `air_quality.csv` $\rightarrow$ `air_quality`
- `weather.csv` $\rightarrow$ `weather`
- Task 5's enriched output $\rightarrow$ `integrated_taxi_trips`

**3. Partitioning Strategy**

- **`trip_data`**: Partition by date (year and month).
- **`weather`**: Partition by year, month, and day.
- **`air_quality`**: Not partitioned — treated as a lookup table (see Design Decisions below).
- **`taxi_zones`**: Not partitioned. It is too small (~265 rows) and there is no key worth partitioning on.
- **`integrated_taxi_trips`**: Partition by year and month, matching `trip_data` since it inherits one row per trip.

---

### Design Decisions

**Which datasets should conceptually be treated as lookup tables?**
The `taxi_zones` and `air_quality` datasets are treated as lookup tables. `taxi_zones` is a small, static dimension table (~265 rows). `air_quality` is also treated as a lookup rather than a partitioned fact table: in the current municipal dataset it covers a single state/county and, as it stands, a single pollutant (PM2.5), so `state_code`, `county_code`, and `parameter_code` are effectively constant — partitioning on them would not narrow scans, only add directory overhead. `weather` is not a lookup table; it is a time-series fact table, which is why it is partitioned by date below.

**Which datasets should not be partitioned? Explain why.**
The `taxi_zones` and `air_quality` datasets should not be partitioned. `taxi_zones` is too small, and partitioning it would make Spark slower (opening many folders to find few items). `air_quality` is currently low-cardinality on every plausible partition column (one state, one county, one pollutant), so partitioning it today would produce very few, unevenly sized partitions without a real scan-reduction benefit — see the 20× scenario below for when this changes.

**Which datasets require different partitioning strategies?**

- **`trip_data`** and **`integrated_taxi_trips`** are partitioned temporally by year and month (although this has to be tested).
- **`weather`** is partitioned temporally as well, but one level finer (year, month, day), since it is a much smaller table where a per-day partition still holds a reasonable number of rows.
- **`air_quality`** and **`taxi_zones`** are left unpartitioned (see above).

**Under what conditions does partitioning become harmful?**
Partitioning becomes harmful when there are too many unique values in the partition column. There must be a balance between the number of folders generated and the amount of data inside each folder.

**If the total data volume increased by 20×, what changes would you make to your storage design?**
Make the partitioning strategy more in depth

- Partition **`trip_data`**: use the day, not just year/month, once monthly partitions get too large.
- Partition **`air_quality`**: at 20× scale it likely stops being a small, low-cardinality lookup table — if the source grows to cover multiple states/counties or more pollutants, partition by `date_gmt` (and `state_code`/`county_code` if the data becomes multi-region) to keep it from becoming one giant unpartitioned table.
- Adjust **`weather`** partition by year (maybe even month) if the per-day granularity produces too many small partitions relative to the larger data volume.

## Task 3. Build a Generic Ingestion Framework

### Which components are generic and reusable across all datasets?

The date and time columns are generic. The location, after some processing, can be generic and reusable across all datasets.

### Which components remain dataset-specific, and why?

- **For air quality:** Datum, parameter name, units of measure, and method name.
- **For the trip data:** The rating and fare amounts.
- **For the taxi_zone_lookup:** Service zone.
- **For the weather:** Sources of temperature, humidity, wind, etc.

### How are transformation rules defined and maintained?

- **General rules:** `readDataGeneric` and `modify_data_generic`, which are reusable for CSV and Parquet files. The modification of the data is done in order to drop rows with bad values (PK is null, the value of a column is negative although it should be positive, timestamps are not well formatted). At the same time, keeping the naming convention to `snake_case`.
- **Dataset-specific rules:** They create the surrogate key and they modify the date and time defined in each dataframe in order to generalize it inside a new field.
- **Maintaining:** Whenever a new dataset is added, generic rules are applied first, then a user needs to define the new dataset-specific rules. In order to be compatible, they would define a datetime column and a PK column. The types of data for each dataset are defined in the schema. If a new column is added, or a type is modified, then the schema needs to be modified as well.

### How are metadata (e.g., schema versions, ingestion statistics) managed?

The metadata is collected through the pipeline function. It defines manually the schema version, and the time and the counting of the records is done inside.

### How does your design reduce code duplication and simplify future maintenance?

The generic function for reading and applying transformations reduces code duplication. At the same time, the pipeline function is doing all the metadata fetching (processing time, number of records before and after processing, version).

### If the municipality adds 20 new datasets next year, what changes would be required to your framework?

Each new dataset would require a dedicated function to define the primary key as well as a timestamp/location column. Other than that, the pipeline would take the dataframe through all required stages.

## Task 4. Design a Common Data Model

**A standard timestamp format**
The standard timestamp format was created for 3/4 datasets. For the weather data, we appended the year, month, and day to the hour and minute. For the air quality, we appended the date with the time formats.
The trip data already had the timestamp, but we extracted the year and month for partitioning.

**Consistent naming conventions**
We decided on the snake format for database columns (hence modified all the names to lowercase, with "\_" instead of " ").

**Rules for handling missing values**
For the weather missing values (primarily in the snow category), the schema allows having null values.
Whenever the timestamp data or the primary key data was missing, we decided to eliminate the row as it could not correlate to the other datasets.
For the taxi trips, we eliminated the negative fare amounts.

**Common data types**
The data types are defined in the schema for each dataset. Hence, all datasets have common data types (Long, Integer, String, Double, Timestamp).

**Document every transformation.**
See section "How are transformation rules defined and maintained?" for more details.

## Task 5. Build the Integration Pipeline

The integration pipeline (`build_integrated_trips`) enriches every cleaned taxi trip with weather, air quality, and pickup/dropoff zone information, and writes the result to `output_data/integrated_taxi_trips`, partitioned by `year` and `month` like the trip table itself. Trips are treated as the anchor entity: every join is a `left` join from `trip_data`, so enrichment can only add columns, never remove or duplicate a trip. This is verified with a sanity check comparing `trip_clean.count()` to the row count of the integrated table after the join.

**How should an hourly weather observation be associated with a taxi trip?**
Each trip's `tpep_pickup_datetime` is truncated down to the hour (`date_trunc("hour", ...)`) to produce `pickup_hour`. The weather table's standardized `timestamp` column (see Task 4) is aliased to `weather_hour`, and the two are joined on equality. Since weather is recorded once per hour, this is a clean 1-to-1 lookup: each trip inherits the temperature, humidity, precipitation, snow depth, wind speed, pressure, and weather condition code (`coco`) in effect during the hour the trip started.

**How should hourly air-quality measurements be associated with a taxi trip?**
Air quality is more complex than weather: a single hour can contain several readings for different pollutants, methods, and sample points across stations. To reduce this to one row per hour, the air-quality data is truncated to `aq_hour` using `timestamp_local`, grouped by `aq_hour`, and pivoted on `parameter_name` with `avg(sample_measurement)` — one output column per distinct pollutant present in the data, each keeping its own unit rather than being averaged together across parameters. This yields exactly one row per hour, which can then be left-joined onto `pickup_hour` without fan-out. The pivoted column names are dynamic (they come from the data), so a small lookup table maps known EPA parameter names to short, readable column names (e.g. `"PM2.5 - Local Conditions"` → `pm25`); any pollutant not in that lookup falls back to an auto-generated slug of its raw name, so the pipeline still works if the source data gains new pollutants. In our dataset, PM2.5 is the only pollutant present, so today `pm25` is the sole air-quality column produced.

**How should missing observations be handled?**
All four enrichment joins (weather, air quality, pickup zone, dropoff zone) are `left` joins. If no matching weather record, air-quality record, or zone lookup exists for a trip's join key, the corresponding columns are left `null` instead of the trip being dropped. This is consistent with the Task 4 rule that trips are never discarded due to a gap in a secondary dataset — only rows missing their own primary key or timestamp are eliminated.

**What are the limitations of your integration strategy?**

- **Hourly granularity**: conditions can change within an hour, but every trip that starts in the same hour is treated as if it experienced identical weather and air quality. Trips near an hour boundary lose precision.
- **Single pollutant in practice**: although the pivot is generic across pollutants, the source `air_quality.csv` only contains PM2.5 readings, so the integrated table currently exposes one air-quality column (`pm25`). The design would automatically add more columns if the source data included other pollutants, but no cross-pollutant index (e.g. a composite AQI) is computed.
- **Cross-station averaging**: `aq_hourly` averages `sample_measurement` across every monitoring site/county reported for that hour, without regard to which station is closest to the trip. A trip's air-quality value is therefore a citywide/regional hourly average, not a reading from its nearest station.
- **No spatial matching**: the join keys are purely temporal (pickup hour), not spatial. Weather and air-quality stations are not necessarily near a trip's actual pickup location, so enrichment reflects citywide/station-level conditions rather than hyperlocal ones.
- **Pickup-time only**: enrichment is computed from the pickup hour only. Conditions at drop-off, which may differ meaningfully for longer trips, are not captured.
- **Timezone assumption**: `timestamp_local` in the air-quality data and the trip timestamps are assumed to already share the same local timezone; no explicit timezone conversion or validation is performed.
