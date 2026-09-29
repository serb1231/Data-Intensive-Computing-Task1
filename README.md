# NYC Taxi Data Integration & Ingestion Framework

This repository contains a PySpark-based data ingestion and integration pipeline. It processes heterogeneous datasets (NYC Taxi Trips, Weather, Air Quality, and Taxi Zones), standardizes them into a Common Data Model, applies data quality checks, and stores them as partitioned Delta Lake tables.

## Project layout

Scripts you run, in the order of section 2 (the step numbers refer to it):

| File | Week | What it does | Step |
| --- | --- | --- | --- |
| `data_ingestion.py` | 1 | Initial load: reads the four sources, validates them, writes the curated Delta tables and the integrated table | 6 |
| `benchmark.py` | 1 | Compares two partitioning strategies for the trips table | 7 |
| `analytical_queries.py` | 2 | The six analytical queries as Spark SQL (also imported by later scripts) | 8 |
| `query_optimization_techniques.py` | 2 | Caching, partition pruning, broadcast join and AQE experiments | 9 |
| `data_products.py` | 2, 3 | Defines the five data products and rebuilds them in full | 10 |
| `simulate_new_data.py` | 3 | Generates the incremental update files into `continuous_data/` (Task 1) | 11 |
| `continuous_data_insert.py` | 3 | Loads the update, re-enriches affected months, refreshes affected products (Tasks 1–2) | 12 |
| `monitoring.py` | 3 | Records every pipeline execution; `python monitoring.py` prints the operational report (Task 3) | 13 |
| `validation.py` | 3 | The validation engine and quarantine; `python validation.py` prints the validation report (Task 4) | 14 |
| `refresh_manager.py` | 3 | Plans and runs the refresh of only the data products that new data affects (Task 2) | 16 |
| `evaluate_platform.py` | 3 | Measures update/refresh time, validation and monitoring overhead, storage (Task 5) | 17 |
| `training_dataset.py` | 4 | Generates the training dataset for hourly taxi demand from the Delta tables (Task 1) | 18 |
| `feature_pipeline.py` | 4 | The reusable feature engineering pipeline: fits it on the train split, writes the feature dataset (Task 2) | 19 |

Configuration and support files, not run directly:

| File | What it holds |
| --- | --- |
| `schemas.py` | Declared schema of every source, including the Week 3 columns `humidity` and `aqi` |
| `validation_rules.py` | The validation rules of every dataset, as configuration (add a rule here) |
| `ml_config.py` | The ML pipeline's configuration: the training dataset (target, features, window, lags, splits) and how the feature pipeline treats each column |
| `tests/` | `test_validation.py` (Week 3 Task 4 fault injection), `test_refresh.py` (Week 3 Task 2 refresh planner), `test_training_dataset.py` (Week 4 Task 1), `test_feature_pipeline.py` (Week 4 Task 2); step 15 |
| `readParquet.py` | Exploratory duplicate analysis from Week 1; not part of the pipeline |
| `environment.yml`, `environment_mac.yml` | Conda environments |

Reports: `Design Report Week N.md` and `Benchmark Report Week 1/2.md` for each week, and
`Evaluation Report Week 3.md` for Week 3, Task 5. Raw measurements are in
`benchmark_results_week_1.json`, `evaluation_results_week_3.json` and, for Week 4,
`training_dataset_week_4.json` and `feature_pipeline_week_4.json`. `Architecture.md` describes
the Week 1 pipeline.
Generated data goes to `output_data/`: the curated tables, `data_products/`, `monitoring/`,
`quarantine/` and `ml/`, all Delta tables and git-ignored.

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
   materialized comparison are in `Benchmark Report Week 2.md`.

   Useful variants:

   ```bash
   python data_products.py --product weather_impact_summary  # rebuild a single product
   python data_products.py --month 2024-03                   # rebuild only March of every product
   python data_products.py --no-cache                        # skip CACHE TABLE, to measure what caching is worth
   ```

   Since Week 3 every product is partitioned by `(year, month)` and covers every month with at
   least 1,000 trips (discovered from the data, no longer hard-coded to January–March 2024).
   `weather_impact_summary` and `borough_mobility_summary` are stored at a monthly grain; their
   Week 2 shape is served by compatibility views of the same name
   (`data_products.register_product_views(spark)`), so Week 2 queries run unchanged. After an
   incremental load, use `refresh_manager.py` (step 16) instead of rebuilding everything.

   To inspect a product's lineage afterwards, every refresh also writes its metadata into the
   Delta commit itself:

   ```sql
   DESCRIBE HISTORY delta.`/absolute/path/output_data/data_products/daily_mobility_summary`
   ```

11. Run  simulate_new_data.py
   This script  creaes new data for the air quality, taxi trips, weather tables. The new data will be created inside the `continuous_data` directory.

   ```bash
   python simulate_new_data.py
   ```

12. Run the continuous_data_insert.py
   This script will integrate the newly generated data inside the existing datalake.
   ```bash
   python continuous_data_insert.py               # load, re-enrich, refresh the affected products
   python continuous_data_insert.py --no-refresh  # load and re-enrich only
   ```

   After the four tables are merged, integrated months whose weather or air quality arrived
   after their trips are rebuilt (re-enrichment), and then only the data products affected by
   the release are refreshed (step 16). A second run of the same release inserts nothing
   (every record is recognised as already loaded) and the refresh skips every product. The
   records it rejects are appended to the quarantine again, though: the quarantine is not yet
   idempotent (see `Evaluation Report Week 3.md`).

   Every batch, initial or incremental, now goes through the Week 3 validation framework
   (`validation.py`, rules in `validation_rules.py`) and is recorded by the monitoring component
   (`monitoring.py`). Nothing has to be called explicitly: `execute_pipeline` does both.

   **Schema evolution** is handled per batch. CSV columns are bound by header name, so a new,
   missing or reordered column cannot shift values into the wrong column. A new column that is
   declared in `schemas.py` (such as `humidity` or `aqi`) is added to the Delta table
   automatically. An undeclared column is dropped and reported. A missing required column, or
   a type that cannot be cast, rejects the batch without writing anything. To accept a new
   column, declare it in `schemas.py` and re-run the load.

13. Print the monitoring report (Week 3, Task 3). It answers the operational questions with
   Spark SQL over `output_data/monitoring/pipeline_runs` and
   `output_data/monitoring/validation_results`:

   ```bash
   python monitoring.py                          # every query
   python monitoring.py --query slowest_datasets # one of: failing_datasets, failing_rules,
                                                 # slowest_datasets, rejections_per_execution,
                                                 # processing_time_trend, schema_history, latest_status
   ```

14. Print the validation report (Week 3, Task 4): the latest outcome of every rule per dataset,
   validation statistics per category, and the contents of the quarantine tables under
   `output_data/quarantine/`:

   ```bash
   python validation.py --samples 5
   ```

   Rejected records keep their own columns plus `_rejection_reasons`, `_malformed_values`
   (the raw text of any value that could not be read as its declared type) and the
   `_run_id`/`_execution_id` of the load that rejected them, so they can be queried directly:

   ```sql
   SELECT _rejection_reasons, COUNT(*) FROM delta.`/absolute/path/output_data/quarantine/trip_data`
   GROUP BY _rejection_reasons ORDER BY 2 DESC
   ```

   To add a validation rule, append it to the dataset's list in `validation_rules.py` (see the
   docstring there). The engine in `validation.py` does not change.

15. Run the tests (about 2 minutes). `tests/test_validation.py` pushes small batches with known
   problems through the real pipeline in a temporary directory and checks that every problem is
   detected, quarantined and recorded. `tests/test_refresh.py` changes a tiny integrated table
   the way a release does (new month, rewritten month, new or lost column, late weather) and
   checks that the refresh planner rebuilds exactly what is affected:

   ```bash
   python -m pytest tests
   ```

   For the Task 5 overhead measurements, two switches run the same pipeline without a component:
   `PLATFORM_VALIDATION=off` (no record-level rules) and `PLATFORM_MONITORING=off` (no
   monitoring writes), e.g. `PLATFORM_MONITORING=off python continuous_data_insert.py`.

16. Refresh the analytical data products after new data (Week 3, Task 2). The planner reads the
   integrated table's Delta log since each product's last refresh and decides, per product,
   between a full rebuild, an incremental rebuild of the changed months, skipping it, or
   blocking it (a column it needs was removed) — and prints why:

   ```bash
   python refresh_manager.py --dry-run   # show the plan only
   python refresh_manager.py             # refresh what the plan says
   python refresh_manager.py --verify    # ...then compare every product with a from-scratch build
   python refresh_manager.py --full      # rebuild every product over the whole window
   ```

   `continuous_data_insert.py` runs the same refresh at the end of every load. The registry
   (`output_data/data_products/_registry`) records each refresh's mode, the partitions it
   rebuilt, the reason, and the source schema it was built from. The strategy is described in
   `Design Report Week 3.md` (Task 2).

17. Reproduce the evaluation (Week 3, Task 5). It needs a fresh initial load (step 6) and the
   update files (step 11), but no update loaded yet:

   ```bash
   python evaluate_platform.py            # about 12 minutes; --reps N for more or fewer trials
   ```

   It loads the update under three configurations (everything on, validation off, monitoring
   off), restoring the tables to their pre-update Delta versions after every trial. Then it
   loads the update for real, times the incremental, no-op and full refreshes, checks the
   incremental result against a from-scratch build, and measures storage before and after.
   Raw measurements are written to `evaluation_results_week_3.json`; results and discussion
   are in `Evaluation Report Week 3.md`. The trials are recorded in the monitoring tables with
   `pipeline = 'evaluation:<config>'`.

18. Generate the Week 4 training dataset (Task 1). It needs the initial load (step 6); the Week 3
   update may or may not be loaded, because the window ends with March:

   ```bash
   python training_dataset.py                     # the dataset declared in ml_config.py, ~30 seconds
   python training_dataset.py --end 2024-03-18    # another window; the splits move with its end
   python training_dataset.py --source-version 0  # rebuild from an older version of the integrated table
   ```

   It writes one row per (pickup zone, local hour) to `output_data/ml/hourly_zone_demand`, a Delta
   table partitioned by `split`: the target `trip_count`, the zone's borough and service zone, the
   weather and PM2.5 of the hour, and the zone's demand 1, 24 and 168 hours earlier. The three splits
   are consecutive whole weeks (train, then validation, then test). Read one with
   `spark.read.format("delta").load("output_data/ml/hourly_zone_demand").where("split = 'train'")`.

   What was built is recorded twice: in the Delta commit (`DESCRIBE HISTORY` on the table) and in
   `training_dataset_week_4.json`. The record lists the source tables and the Delta versions read,
   the window and split boundaries, the size of each split, the NULL share of every feature, a
   dictionary of the columns and a measure of how much each source tells about demand. To change
   the target, a feature, a lag, the window or the splits, edit `ml_config.py`; the builder does not
   change. The design is described in `Design Report Week 4.md` (Task 1).

19. Run the Week 4 feature engineering pipeline (Task 2). It needs the training dataset (step 18):

   ```bash
   python feature_pipeline.py              # fit on train, write the features; ~1.5 minutes
   python feature_pipeline.py --no-probe   # the same without the random-forest probe; ~20 seconds
   ```

   The pipeline is a Spark ML `Pipeline` of standard stages, fitted on the train split only:

   - calendar features derived from `pickup_hour`;
   - `log1p` of the lags and a 0/1 flag for every quantity with missing values;
   - median imputation, one-hot encoding of the codes, and scaling of the quantities;
   - one `features` vector (293 slots).

   It writes `output_data/ml/hourly_zone_demand_features` (Delta, partitioned by `split`: keys,
   `trip_count`, `features`) and saves the fitted pipeline to
   `output_data/ml/models/hourly_zone_demand_features`, loadable with `PipelineModel.load`.
   `feature_pipeline_week_4.json` records the plan (which column goes through which stage and what
   was dropped), what the stages learned (medians, means, standard deviations, categories), the
   name of every slot, and a probe of how much each source dataset contributes.

   To reuse the stages in a model (Task 3):

   ```python
   from pyspark.ml import Pipeline
   from pyspark.ml.regression import GBTRegressor
   from feature_pipeline import feature_pipeline

   features, plan = feature_pipeline(train)  # train: the train split of the training dataset
   model = Pipeline(stages=features.getStages() + [GBTRegressor(labelCol="trip_count")]).fit(train)
   ```

   A new feature needs one line in `ml_config.py`: its name in the dataset section, and its kind
   (code, flag, count to log-transform) if it is not a plain quantity. A feature that is mostly
   missing or constant in the train split is dropped automatically and reported.

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
- `simulate_new_data.py` has the same `fastparquet` dependency and also writes into `.tmp/`
  and `continuous_data/` without creating them.
- An environment created before Week 3 lacks the `delta-spark` Python package, which
  `data_ingestion.py` imports. Install it with `pip install delta-spark==3.1.0`, which also
  installs `importlib_metadata`. Both environment files now declare it. The same applies to
  `pytest` (step 15): `pip install pytest`.
