# Design report for week 3

## Task 1. Generating Incremental Update Datasets

This task consisted of creating some new data based on the existing datasets for the taxi trips, air quality and weather conditions. The script 'simulate_new_data.py' does this.

* **Taxi Trips ->** For this, we parsed the initial data in order to get the trips with positive fees, afterwards find the maximum timestamp for the trips, and based on that, copy 10% of the data and only modify the timestamps to happen afterwards.
* **Weather Data ->** Given that this data spans a year, we only had to increment it by 1, in order to produce the new data.
* **Air quality ->** For this one, we got all the data, modified the timestamps to be 1 year later, and created the aqi based on the
* sample measurement.



**the number of new records,**
**the number of duplicate records (where applicable),**

* Number of taxi trips created: 2053913
* Number of taxi trips duplicates created: 0
* Number of weather records created: 8784
* Number of air quality records created: 8139551

**the schema changes introduced.**

* Air quality schema introduced:
```python
StructField("aqi", IntegerType(), True)

```


* weather schema:
```python
StructField("humidity", IntegerType(), True)

```



---

Another thing that needed to be done is to define the data ingresion to be permmissive in the anterior version (week 1).

### Implement an incremental update pipeline that:

The pipeline was built inside the 'continuous_data_insert.py' file. We used the functions 'readDataGeneric' and 'execute_pipeline' From data_ingresion.py file.

* **inserts new records,**
Inside the readDataGeneric function, we changed th schema validation to Permisive. This would mean that we would have backwards compatibility.
Given that the new data contains an extra column, that mean that the old data would not have some columns, and would need to fill it as NA.
* **ignores duplicate records,**
THis is being done automatically. We decided to start using DeltaTable library from Spark. This would help with the process. We would no longer need to read the whole dataset inside the dataframe in order to afterwards modify it.
It would be able to directly verify the data that is beig duplicated, the amount of data that could be inserted
* **preserves unchanged records,**
The records are preserved unchanged through the
```python
    (target.alias("t")
           .merge(clean_df.alias("s"), f"t.{pk_col} = s.{pk_col}")
           .whenNotMatchedInsertAll()
           .execute())

```


* **supports schema evolution & updates the corresponding Delta tables without rebuilding the platform.**
Schema evolution is being supported through the Permisive flag.


## Task 3. Build a Platform Monitoring System

### Monitoring architecture

Monitoring is implemented in `monitoring.py` and runs without being called. `execute_pipeline` runs its whole body inside `monitoring.track(...)`, a context manager that records the execution when the block ends. It records successful executions, batches rejected because of their schema, and executions that raise an exception (status `failed`, with the error message, after which the exception is re-raised). Any dataset that goes through `execute_pipeline` is therefore monitored, and a pipeline script only labels its run with one line, for example `monitoring.start_run("incremental_update")`.

Everything is stored in two append-only Delta tables under `output_data/monitoring/`:

| Table | Grain | Main columns |
| --- | --- | --- |
| `pipeline_runs` | one row per dataset per execution | `run_id`, `execution_id`, `pipeline`, `dataset`, `source`, `status`, `error_message`, `started_at`, `finished_at`, `execution_time_seconds`, `validate_seconds`, `write_seconds`, `schema_version`, `schema_fingerprint`, `schema_changes`, `source_records`, `out_of_scope_records`, `processed_records`, `rejected_records` (= `quarantined_records` + `dropped_records`), `warning_records`, `inserted_records`, `validation_failures`, `target_version` |
| `validation_results` | one row per check per execution | `rule_name`, `category`, `action`, `status` (passed / failed / warning / info / skipped), `failed_records`, `checked_records`, `details` |

Some design choices:

* **Delta rather than a log file.** The metadata lives in the same storage as the data, appends are atomic, and the operational questions are plain Spark SQL. After two full runs both tables together take 380 KB.
* **Schema versions are derived, not typed in.** Week 1 hard-coded `"schema_version": "1.0"`. Each source schema (column names and types, order-insensitive because CSV columns are now bound by name) is now fingerprinted. The first schema seen for a dataset is version 1, and every schema not seen before gets the next number. Weather moved to version 2 when `humidity` appeared, air quality when `aqi` appeared, and trips when `passenger_count` and `ratecodeid` arrived as `double` instead of `bigint`.
* **`target_version`** is the Delta version the execution committed. It links every monitoring row to `DESCRIBE HISTORY` of the table, and to `RESTORE ... TO VERSION AS OF` if a load has to be undone.
* **Monitoring time is excluded from `execution_time_seconds`.** The Delta appends happen after the timer stops. `PLATFORM_MONITORING=off` skips them entirely, so Task 5 can measure the overhead by running the same load both ways.

### Operational queries

`python monitoring.py` runs the queries in `MONITORING_QUERIES`. The answers below come from the initial load followed by the Task 1 update:

| Question | Query | Answer on our data |
| --- | --- | --- |
| Which dataset fails validation most frequently? | `failing_datasets` (drill-down: `failing_rules`) | Trip Data: both executions had failures, 17 failed checks, 12.5% of records rejected. Air Quality rejected the highest share in a single execution (99.9% of the update, see Task 4). |
| Which dataset requires the longest processing time? | `slowest_datasets` | Trip Data: 34.8 s on average, 47.3 s at most. The integrated table follows with 31.4 s. |
| How many records were rejected during each execution? | `rejections_per_execution` | Initial load: 1 weather record and 1,074,368 trips. Update: 1 weather record, 51,671 air-quality records and 376,864 trips (209,115 quarantined, 167,749 re-deliveries dropped). |
| How has processing time changed over multiple executions? | `processing_time_trend` | Trips went from 47.3 s (9.55 M records) to 22.3 s (2.05 M records), but throughput fell from 202 k to 92 k records/s. A smaller batch does not amortise the fixed cost of the merge and the key lookup against the existing table. |

Two more queries, `schema_history` and `latest_status`, report when each schema version first appeared and the latest execution of every dataset. Together they work as a health overview.

### Which operational metrics are most useful?

* **Rejection rate, not the rejected count.** 376,864 rejected trips and 51,671 rejected air-quality rows look alike, but 18% and 99.9% of a batch are very different signals. A rate that jumps compared with the previous execution is the clearest sign that a producer changed something.
* **Throughput (records/s) rather than raw execution time.** Batches differ in size by a factor of 1,000, so a longer execution time on its own says little. Throughput is comparable across executions.
* **Failed checks per rule** (`validation_results`). A dataset-level failure count says that something is wrong; the rule name says what.
* **Schema version and changes.** Most production incidents in a pipeline like this start with an upstream schema change. Here the change is recorded on the execution where it first appeared.
* **Status and error message.** They are the first thing to look at when a load did not produce data.

### How does this support debugging and maintenance?

The monitoring data led us to three real problems while building this task:

1. `failing_rules` showed `unique(timestamp)` failing for weather in both runs. The quarantined rows (2024-03-31 and 2025-03-30, 03:00) pointed at the cause. The weather timestamp is built in the Spark session's time zone, and on European daylight-saving days the non-existent 02:00 is shifted onto 03:00. Week 1's `drop_duplicates` removed one of the two observations without a trace.
2. `rejections_per_execution` showed 99.9% of the air-quality update rejected, and the drill-down named `well_formed`. The Task 1 generator writes `aqi` as a min-max-scaled float (e.g. `1.3149`) while the schema declares an integer.
3. `write_seconds` exposed a performance regression in our own code. In the first version, the 8,784-row weather table needed 28.6 s to write while 51,885 air-quality rows took 2.2 s. The weather table had been written as 7,372 files instead of 366, because the cached, validated batch kept 64 shuffle partitions. After the fix (repartitioning by the partition columns before the first write) weather takes 8.2 s.

For maintenance, the `processing_time_trend` query is the early warning for capacity problems (throughput falling as the tables grow), and `target_version` makes it possible to roll a table back to the version before a bad load.

### What additional monitoring would a production system need?

* **Alerting.** Thresholds on rejection rate, throughput and status relative to a rolling baseline, pushed to an on-call channel. Today someone has to run the report.
* **Freshness.** The newest event time per table compared with the current time, and the delay between a file arriving and its data being queryable.
* **Table health.** File count, average file size and table size from `DESCRIBE DETAIL` after every write. This would have caught the small-files regression directly instead of through its side effect on write time.
* **Data profiles.** Null rate, min/max and distinct counts per column, per batch, to detect drift that no individual rule catches (e.g. average fare dropping by 30%).
* **Resource metrics.** Shuffle, spill, peak memory and failed tasks per stage, collected with a Spark listener. This is what explains a throughput drop.
* **Lineage and retention.** A checksum of each source file, so a re-delivered file is recognised before it is read, and a retention policy for the monitoring and quarantine tables.


## Task 4. Extend the Data Validation Framework

### Design

The framework lives in `validation.py` (the engine) and `validation_rules.py` (the configuration). Every batch is checked at two levels before anything is written.

**Dataset level: schema changes.** Two comparisons run before the batch is processed:

* *Source against contract.* The contract is the dataset's declared schema in `schemas.py` plus a list of required columns. A required column that is missing rejects the batch. A column the contract does not declare is an *unexpected* change: it is dropped and reported (the policy is configurable per dataset).
* *Batch against the Delta table.* A new declared column is added to the table (Delta schema auto-merge). A column the table has but the batch lacks is filled with NULL. A numeric or temporal type change is cast back to the table's type. Any other type change (e.g. a string where a timestamp was) rejects the batch.

A rejected batch writes nothing, is recorded with status `rejected`, and returns an empty DataFrame shaped like the table, so the next steps (the integrated table) process zero rows instead of failing. CSV files are now read as text and bound to columns by **header name**. Week 1 bound them by position, so a column inserted in the middle of a new release would have shifted every later value into the wrong column.

**Record level: rules.** A rule is an object with a name, a category, an action and a `violated()` predicate:

| Category (Task 4) | Rules |
| --- | --- |
| duplicate records | `unique(pk)` within the batch (keeps the copy that passes the other rules); `not_already_loaded(pk)` against the table |
| invalid attribute values | `well_formed` (value cannot be read as its declared type), `in_range`, `positive`, `allowed_values`, SQL checks such as `dropoff_after_pickup` |
| missing reference records | `reference(pulocationid -> taxi_zones.locationid)` and the same for `dolocationid`; for the integrated table, trips without a weather or air-quality observation for their hour |
| unexpected or unsupported schema changes | the dataset-level checks above |
| incomplete records | `not_null(...)` on every mandatory attribute |

The action decides what a violation costs. `quarantine` isolates the record, `drop` excludes and counts it without storing it, and `warn` keeps it and reports it. `drop` is used only for `not_already_loaded`: an exact re-delivery is expected in an at-least-once feed, and storing it would refill the quarantine with millions of rows every time a load is re-run.

The engine evaluates all rules in one projection. Every row gets an array of the rule names it breaks per action, so a row that breaks three rules is reported with all three reasons. Rules that need a window or a join (`unique`, `not_already_loaded`, `reference`) run as a second stage, after the row-level rules, so that `unique` can prefer the copy that passed them. The flagged batch is materialized once with a local checkpoint, and a single aggregation then computes every rule's count. The checkpoint also cuts the lineage. With a plain `persist()`, Delta invalidated the cache when the batch was merged into the table, and the batch handed to the integrated table was re-validated against the new table version, where every row was "already loaded". The integrated update received 0 of 1,677,049 trips until we fixed it, and a test now guards against it.

**Invalid records** meet the four requirements as follows:

* They do not interrupt processing. A rule never raises; it flags. A rule that cannot run on a batch, for example because the batch lacks the column it reads, is recorded as `skipped` with the reason.
* They are isolated in a per-dataset Delta quarantine table (`output_data/quarantine/<dataset>`). The table keeps the record's own columns and types plus `_rejection_reasons`, `_malformed_values` (the raw text of any value that failed to convert, e.g. `aqi=1.3149`) and the run and execution ids.
* They are reported in `validation_results` (one row per rule per execution) and by `python validation.py`.
* They are excluded from analytical data products because only the valid rows reach the curated tables. The integrated table is built from the clean trips batch, and the Week 2 data products are built from the integrated table.

Week 1's `modify_data_generic` has been removed. Its filters (positive values, non-null timestamps and keys, de-duplication) are now rules in `validation_rules.py`, so the process functions only derive columns.

### Results on our data

| Execution | Processed | Quarantined | Dropped | Inserted | Main reasons |
| --- | --- | --- | --- | --- | --- |
| Trips, initial load | 9,554,778 | 1,074,368 (11.2%) | 0 | 8,480,410 | `not_null(passenger_count)` 751,962; `positive(trip_distance)` 215,764; `non_negative_amounts` 136,908; `unique(surrogate_key)` 116,013; `positive(passenger_count)` 105,931 |
| Trips, update | 2,053,913 | 209,115 | 167,749 | 1,677,049 | re-deliveries of loaded trips (dropped); `not_null(passenger_count)` 165,415; `positive(trip_distance)` 42,899 |
| Weather, initial / update | 8,784 / 8,760 | 1 / 1 | 0 | 8,783 / 8,759 | `unique(timestamp)` (daylight-saving collision) |
| Air quality, initial / update | 51,885 / 51,714 | 0 / 51,671 | 0 | 51,885 / 43 | `well_formed`: `aqi` is not an integer |
| Integrated trips, initial | 8,480,410 | 0 | 0 | 8,480,410 | 14 warnings: 10 trips dated 2023 and 4 dated 2009, outside the weather and air-quality coverage |

The quarantine takes 75 MB, against 1.3 GB of curated tables. Besides the weather time-zone problem, the framework found three issues that the Week 1 pipeline hid:

* **Wrong duplicate kept.** TLC refunds reuse the original trip's key with negative amounts. Week 1's `drop_duplicates` kept an arbitrary copy, and its trips table contained 105,141 rows with negative fares. The new table has almost the same row count (8,480,410 against 8,480,540), but `unique` now keeps the paying trip, and the refund is quarantined with both reasons.
* **Silent NULLs from the CSV reader.** pandas writes an integer column that contains a single NaN as floats, so the weather update has `coco` values such as `3.0`. Spark's typed CSV parser in `PERMISSIVE` mode turns these into NULL for all 8,760 rows without a warning. The framework's conversion accepts `3.0` as the integer 3 and flags only lossy values, such as `12.5` in an integer column.
* **Type mismatch in the Task 1 air-quality update.** `aqi` is generated as a float scaled to 0–100, while `schemas.py` declares an integer AQI (and a real AQI is an integer). The old reader would have loaded the column as NULL. The framework quarantines the rows with the raw values preserved. This needs a decision from the team: either declare `aqi` as `DoubleType`, or generate a real integer AQI from the PM2.5 breakpoints. Once fixed, the quarantined rows can be re-submitted.

### Which validation rules are generic, and which are dataset-specific?

Generic rules apply to every dataset without being configured, because they depend only on the pipeline's own metadata. They are `well_formed` (for any source with declared types), `not_null` and `unique` on the primary key, `not_already_loaded`, and both schema comparisons. A dataset without a contract still gets all of them. The rule *types* (`NotNull`, `InRange`, `Positive`, `AllowedValues`, `ReferenceExists`, `Check`) are generic too; only their parameters are dataset-specific.

Dataset-specific rules encode domain knowledge:

* **Weather:** physical plausibility bounds (temperature, pressure, humidity) and the Meteostat condition codes.
* **Trips:** the TLC data dictionary's vendor, rate and payment codes; cross-field logic such as `dropoff_after_pickup` and `trip_under_24h`; and the foreign keys to the zone lookup.
* **Air quality:** EPA's tolerance of slightly negative PM2.5 readings, and the plausible offset between local and GMT timestamps.
* **Integrated table:** warning-level checks, because a trip without weather is still a valid trip.

### How can new rules be added without modifying the core framework?

Rules are configuration. Adding one means appending a line to the dataset's list in `validation_rules.py`, for example `InRange("fare_amount", 0, 1000)` or `Check("tip_below_fare", "tip_amount <= fare_amount")`, which accepts any Spark SQL condition. A rule type that does not exist yet is a subclass of `Rule` with a `violated()` method (and `prepare()` if it needs a join or a window), defined anywhere and listed in the configuration. The engine iterates over whatever rules the contract returns and never names a rule.

Two properties keep this safe:

* A rule on a column that is not in the batch is skipped and reported rather than failing. The `humidity` and `aqi` rules were declared before any file contained those columns.
* Rule names must be unique within a dataset. This guard caught a real duplicate while we built the framework: `not_null(timestamp)` was listed explicitly as well as generated from the primary key.

Every rule category is covered by the fault-injection tests (`python -m pytest tests`). They push small batches with known problems through the real `execute_pipeline` in a temporary directory.

### Trade-offs

* Evaluating every rule in one pass is cheaper than one Spark job per rule, but it materializes the whole batch. At 9.5 M trips this means local-checkpoint blocks on local disk. At much larger scale we would validate per partition (per month) instead.
* Quarantining keeps the evidence but costs storage (75 MB here). Dropping exact re-deliveries instead of storing them is a deliberate exception.
* Delta `CHECK` constraints were considered and rejected: they fail the whole write on the first bad row, which contradicts "isolate and continue".
* Skipping rules whose columns are absent makes the framework tolerant of schema evolution. The risk is that a misspelled column name leaves a rule skipped forever, which is why `skipped` is a visible status in `validation_results` rather than a silent no-op.
