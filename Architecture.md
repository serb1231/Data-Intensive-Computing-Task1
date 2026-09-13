# Architecture Diagram

Diagram of the data platform implemented in `data_ingestion.py`. Nesting in the diagram
mirrors nesting in the code: `execute_pipeline` is the outer orchestrator, it calls
`process_func`, and `process_func` calls `modify_data_generic` itself. `readDataGeneric`
runs before any of that, at module level.

```mermaid
flowchart TB

    subgraph SRC["Raw sources — data/"]
        direction LR
        S1[/"weather.csv"/]
        S2[/"air_quality.csv"/]
        S3[/"taxi_zone_lookup.csv"/]
        S4[/"yellow_tripdata_2024-01 / -02 / -03.parquet"/]
    end

    READ["readDataGeneric() — module level, before any pipeline call<br/>CSV: read with the declared schema, mode=FAILFAST<br/>Parquet: embedded schema is used, the declared one is ignored<br/>then every column: lower() and spaces to underscores"]
    UNION["unionByName() — the three monthly parquets into one DataFrame"]

    S1 --> READ
    S2 --> READ
    S3 --> READ
    S4 --> READ
    READ -- "trips only" --> UNION

    subgraph PIPE["execute_pipeline(dataset, raw_df, process_func, path, partition_cols, scope_func)"]
        direction TB
        CNT1["source_count = raw_df.count()"]
        SCOPE["scope_func(raw_df) — optional, air quality only<br/>air_quality_scope: state_code = 36 and county_code in 005/047/081<br/>counted as out_of_scope_records, never as rejections"]

        subgraph PROC["process_func() — the only dataset-specific code"]
            direction TB
            SPEC["weather_data_process / air_quality_process /<br/>trip_data_process / taxi_zones_data_process<br/>build timestamp, surrogate key, year+month"]
            MOD["modify_data_generic() — called from inside process_func<br/>drop null PK, drop null timestamp,<br/>drop non-positive numeric columns, dedupe on PK"]
            SPEC --> MOD
        end

        WRITE["write Delta — mode=overwrite, overwriteSchema=true<br/>partitionBy(partition_cols) when given"]
        CNT2["clean_count = clean_df.count()"]
        META["emit metadata — schema_version, source_records, out_of_scope_records,<br/>processed_records, rejected_records, final_clean_records,<br/>execution_time_seconds"]

        CNT1 --> SCOPE --> PROC --> WRITE --> CNT2 --> META
    end

    READ --> CNT1
    UNION --> CNT1

    subgraph LAKE["Delta Lake — output_data/"]
        direction LR
        T1[("weather<br/>year, month, day")]
        T2[("air_quality<br/>unpartitioned")]
        T3[("taxi_zones<br/>unpartitioned, lookup")]
        T4[("trip_data<br/>year, month")]
    end

    WRITE --> T1
    WRITE --> T2
    WRITE --> T3
    WRITE --> T4

    CLEAN["DataFrames returned by execute_pipeline:<br/>weather_clean, air_quality_clean, taxi_zones_clean, trip_clean<br/>lazy lineages back to the source files, NOT reads of the tables above"]
    META --> CLEAN

    subgraph PIPE2["execute_pipeline() again — process_func = build_integrated_trips"]
        direction TB
        JOIN["build_integrated_trips(trips, weather, air_quality, taxi_zones)<br/>pickup_hour = date_trunc hour of tpep_pickup_datetime<br/>LEFT JOIN weather on pickup_hour = weather_hour<br/>LEFT JOIN air quality on pickup_hour = aq_hour, pivoted per parameter_name<br/>LEFT JOIN taxi_zones twice, on pulocationid and on dolocationid<br/>modify_data_generic is NOT applied on this path"]
        WRITE2["write Delta — partitionBy(year, month)"]
        JOIN --> WRITE2
    end

    CLEAN --> JOIN

    WRITE2 --> OUT[("integrated_taxi_trips<br/>year, month — one row per trip, enriched with<br/>weather, pm25, pickup/dropoff zone and borough")]

    SANITY["print trip_clean.count() vs integrated_clean.count()<br/>runs after the write, and is a print, not an assertion"]
    OUT --> SANITY
```

## Reading the diagram

- **Read happens outside the pipeline.** `readDataGeneric` is called at module level for all
  four sources before `execute_pipeline` runs. Schema validation is asymmetric: the three CSVs
  are read with the declared schema under `mode="FAILFAST"`, so a mismatch aborts ingestion,
  while the parquet branch ignores the declared schema entirely — so `trips_schema` is never
  enforced and the largest dataset is the least validated. Name normalization is
  `lower()` plus spaces-to-underscores, which lowercases `PULocationID` to `pulocationid`
  rather than splitting it into words.
- **`execute_pipeline` wraps everything else.** It counts the source, optionally narrows it
  with `scope_func`, calls `process_func`, writes Delta, counts the result and emits metadata.
  Only air quality passes a `scope_func`; its 8,087,666 out-of-region rows are reported as
  `out_of_scope_records` so they never look like data-quality failures.
- **The dataset-specific layer is one function deep.** `process_func` is the only code that
  differs per dataset — it builds the timestamp and the key, then calls the generic
  `modify_data_generic` itself. That is why `modify_data_generic` is drawn inside
  `process_func` rather than after it.
- **The integration pass reuses the same orchestrator.** `build_integrated_trips` is handed to
  `execute_pipeline` as a `process_func`, so the integrated table gets the same write, count
  and metadata treatment — but no `modify_data_generic`, since enrichment adds columns rather
  than filtering rows.
- **The joins do not read the Delta tables.** They consume the DataFrames `execute_pipeline`
  returned, which are lazy lineages back to the raw files. Building `integrated_taxi_trips`
  therefore re-executes the whole ingestion for all four datasets instead of reading what was
  just written — the clearest optimisation available in the current design.
- **The sanity check runs after the write, not before it.** Every enrichment is a `left` join
  from `trip_data`, so no trip can be dropped or duplicated; the row-count comparison at the
  end confirms this, but it is a `print` on an already-written table, not a gate.
