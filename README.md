# NYC Taxi Data Integration & Ingestion Framework

This repository contains a PySpark-based data ingestion and integration pipeline. It processes heterogeneous datasets (NYC Taxi Trips, Weather, Air Quality, and Taxi Zones), standardizes them into a Common Data Model, applies data quality checks, and stores them as partitioned Delta Lake tables.

## 1. Prerequisites

To run this pipeline, you must have the following installed on your machine:

- **Conda** (Miniconda, Anaconda, or Miniforge)
- **Java 17** — you do _not_ need to install this separately. Both environment files pin
  `openjdk=17`, so Conda provides the JVM inside the environment and points `JAVA_HOME` at
  it on activation. Spark 3.5 does not support Java 21+, so letting Conda own the JVM also
  protects you from a newer system-wide JDK on your `PATH`.

## 2. Environment Setup

We manage dependencies using Conda. To guarantee the pipeline runs smoothly, recreate the exact environment using the provided `environment.yml` file. (or environment_mac.yml for MacOS)

1. Open your terminal and navigate to this project's root directory.
2. Create the environment from the YAML file:

   ```bash
   conda env create -f environment.yml
   ```

   or

   ```bash
   conda env create -f environment_mac.yml
   ```

3. Activate the environment:

   ```bash
   conda activate nyc_taxi_pipeline
   ```

4. Copy the datasets into the `data/` folder. The pipeline expects these exact filenames:

   | File                                   | Notes                                  |
   | -------------------------------------- | -------------------------------------- |
   | `data/weather.csv`                     |                                        |
   | `data/air_quality.csv`                 | Ships as `air_quality.zip`; see step 5 |
   | `data/taxi_zone_lookup.csv`            |                                        |
   | `data/yellow_tripdata_2024-01.parquet` |                                        |
   | `data/yellow_tripdata_2024-02.parquet` |                                        |
   | `data/yellow_tripdata_2024-03.parquet` |                                        |

5. Unzip the air-quality archive and rename it. The archive contains
   `hourly_88101_2024.csv`, but the pipeline reads `data/air_quality.csv`:

   ```bash
   cd data && unzip -o air_quality.zip && mv hourly_88101_2024.csv air_quality.csv && cd ..
   ```

   Expect roughly 2.4 GB uncompressed, so make sure you have ~4 GB free before running.

6. Run the script:

   ```bash
   python data_ingestion.py
   ```

   The first run downloads the Delta Lake jar (`io.delta:delta-spark_2.12:3.1.0`) from Maven
   Central, so it needs network access. Later runs reuse the cached jar in `~/.ivy2`.

   This writes the Delta tables into `output_data/` and takes roughly 3 minutes.

7. Optionally run the Task 6 benchmark, which requires step 6 to have completed:

   ```bash
   python benchmark.py
   ```

   It rewrites the trips table under two partitioning schemes into `benchmark_output/`
   (~930 MB, git-ignored), measures ingestion time, storage size, file count and query
   latency, prints a summary, and writes raw measurements to `benchmark_results_week_1.json`.
   Takes about 1 minute. Results and discussion are in `Benchmark Report Week 1.md`.

8. Run the Week 2 analytical query library, which also requires step 6 to have completed:

   ```bash
   python analytical_queries.py
   ```

   It runs all six analytical queries from `Design Report Week 2.md` (Task 1) as Spark SQL
   against `output_data/integrated_taxi_trips`, plus the underlying-table variants of
   queries 1 and 6 against `trip_data`/`taxi_zones`, and prints every result set. The
   query functions are also importable (`from analytical_queries import QUERIES,
   run_all_queries`) for reuse by the optimization experiments and data products in later
   tasks.

9. Run the Week 2 optimization benchmark!

   ```bash
   python query_optimization_techniques.py
   ```
   
   It runs the optimizations described in the test:
- Caching frequently accessed tables or intermediate results.
- Partition pruning by designing queries that read only the required partitions.
- Broadcast joins when joining the large Taxi Trips table with the small Weather, Air Quality, or Taxi Zone Lookup tables.
- Adaptive Query Execution (AQE) by comparing query performance with AQE enabled and disabled.





10. Generate the Week 2 reusable analytical data products (Task 4), which also requires
   step 6 to have completed:

   ```bash
   python data_products.py
   ```

   It builds five Delta tables under `output_data/data_products/` — `daily_mobility_summary`,
   `taxi_zone_statistics`, `weather_impact_summary`, `air_quality_impact_summary` and
   `borough_mobility_summary` — appends one row per refresh to the `_registry` Delta table
   next to them, and then re-answers analytical queries 2, 4 and 5 straight from the products
   to show they replace the ad-hoc SQL. The whole run takes about 80 seconds, most of it the
   single cached scan of the integrated table; the five aggregations on top take ~8 seconds
   and produce ~171 KB in total. The design rationale for each product is in
   `Design Report Week 2.md` (Task 4); storage overhead, build times and the on-demand versus
   materialized comparison are in `Week2 Benchmark Report.md`.

   Useful variants:

   ```bash
   python data_products.py --product weather_impact_summary  # refresh a single product
   python data_products.py --month 2024-03                   # rebuild only March in the daily summary
   python data_products.py --no-cache                        # skip CACHE TABLE, to measure what caching is worth
   ```

   To inspect a product's lineage afterwards, every refresh also writes its metadata into the
   Delta commit itself:

   ```sql
   DESCRIBE HISTORY delta.`/absolute/path/output_data/data_products/daily_mobility_summary`
   ```

## 3. Apple Silicon (macOS arm64) notes

Verified end to end on an M2 Pro (16 GB RAM, macOS 26.5) using `environment_mac.yml`:

- Use **Miniforge**, which has native arm64 builds. Installing via Homebrew at `/usr/local`
  gets you an x86_64 Conda running under Rosetta:

  ```bash
  curl -fsSL -o /tmp/miniforge.sh \
    https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-MacOSX-arm64.sh
  bash /tmp/miniforge.sh -b -p "$HOME/miniforge3"
  ```

- If your shell profile exports a global `JAVA_HOME` (e.g. pinned to Java 25), you can leave
  it alone. `conda activate` runs `openjdk_activate.sh`, which overrides `JAVA_HOME` to point
  at the environment's Zulu JDK 17, and Spark honours `JAVA_HOME` over `PATH`.
- Full run takes roughly 4 minutes and produces about 1.4 GB under `output_data/`.

## 4. Known gaps

- `readParquet.py` (exploratory duplicate analysis, not part of the pipeline) calls
  `pd.read_parquet(..., engine='fastparquet')`, but `fastparquet` is in neither environment
  file. Either add it or switch the engine to `pyarrow`. It also writes into a `.tmp/`
  directory that it does not create.
