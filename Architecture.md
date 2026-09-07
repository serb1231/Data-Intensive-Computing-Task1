# Architecture Diagram

Diagram of the data platform implemented in `data_ingestion.py`: raw source files flow through the generic ingestion framework, dataset-specific transformation rules, and Delta Lake storage, then are combined by the integration pipeline into `integrated_taxi_trips`.

```mermaid
flowchart TB

    subgraph SRC["Raw Sources"]
        direction LR
        S1[/"weather.csv"/]
        S2[/"air_quality.csv"/]
        S3[/"taxi_zone_lookup.csv"/]
        S4[/"yellow_tripdata_2024-01/02/03.parquet"/]
    end

    subgraph FRAMEWORK["Generic Ingestion Framework"]
        direction TB
        READ["readDataGeneric()\nload CSV/Parquet - schema validation (FAILFAST) - normalize column names to snake_case"]
        SPECIFIC["Dataset-specific process_func\nweather_data_process / air_quality_process /\ntrip_data_process / taxi_zones_data_process\n(build timestamps, surrogate keys, derived cols)"]
        MODIFY["modify_data_generic()\ndrop rows: null PK - null/invalid timestamp -\nnon-positive numeric cols - dedupe on PK"]
        PIPE["execute_pipeline()\ncount raw -> process -> write Delta -> count clean\n-> emit ingestion metadata (schema_version,\nprocessed/rejected/clean counts, exec time)"]
        READ --> SPECIFIC --> MODIFY --> PIPE
    end

    subgraph LAKE["Delta Lake Storage (output_data/)"]
        direction LR
        T1[("weather\npartitioned: year, month, day")]
        T2[("air_quality\n(unpartitioned)")]
        T3[("taxi_zones\n(unpartitioned, lookup)")]
        T4[("trip_data\npartitioned: year, month")]
    end

    subgraph INTEGRATE["Integration Pipeline (Task 5)"]
        direction TB
        JOIN["build_integrated_trips()\ntrips LEFT JOIN weather   on pickup_hour = weather_hour\ntrips LEFT JOIN air_quality on pickup_hour = aq_hour (pivoted per pollutant)\ntrips LEFT JOIN taxi_zones  on PU/DOLocationID (pickup + dropoff)"]
        SANITY["sanity check:\ntrip_clean.count() == integrated.count()"]
        JOIN --> SANITY
    end

    OUT[("integrated_taxi_trips\npartitioned: year, month\none row per trip, enriched with\nweather + pm25 + pickup/dropoff zone & borough")]

    S1 --> READ
    S2 --> READ
    S3 --> READ
    S4 --> READ

    PIPE --> T1
    PIPE --> T2
    PIPE --> T3
    PIPE --> T4

    T4 --> JOIN
    T1 --> JOIN
    T2 --> JOIN
    T3 --> JOIN

    SANITY --> OUT
```

## Reading the diagram

- **Raw Sources → Generic Ingestion Framework**: every dataset, regardless of format (CSV or Parquet), passes through the same three generic stages — schema-validated read + name normalization, dataset-specific transformation, then generic data-quality filtering/deduplication — before being timed and written by `execute_pipeline`.
- **Delta Lake Storage**: each cleaned dataset lands as its own Delta table under `output_data/`, partitioned according to the strategy in Task 2 (`weather` and `trip_data` are the only partitioned tables; `air_quality` and `taxi_zones` are not).
- **Integration Pipeline**: `trip_data` is the anchor table. All enrichment (weather, air quality, pickup/dropoff zone and borough) is attached via `left` joins keyed on the trip's pickup hour or location IDs, so no trip is ever dropped or duplicated — verified by the row-count sanity check before the result is written to `integrated_taxi_trips`.
