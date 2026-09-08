# NYC Taxi Data Integration & Ingestion Framework

This repository contains a PySpark-based data ingestion and integration pipeline. It processes heterogeneous datasets (NYC Taxi Trips, Weather, Air Quality, and Taxi Zones), standardizes them into a Common Data Model, applies data quality checks, and stores them as partitioned Delta Lake tables.

## 1. Prerequisites

To run this pipeline, you must have the following installed on your machine:

- **Conda** (Miniconda, Anaconda, or Miniforge)
- **Java 17** — you do *not* need to install this separately. Both environment files pin
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

   | File | Notes |
   | --- | --- |
   | `data/weather.csv` | |
   | `data/air_quality.csv` | Ships as `air_quality.zip`; see step 5 |
   | `data/taxi_zone_lookup.csv` | |
   | `data/yellow_tripdata_2024-01.parquet` | |
   | `data/yellow_tripdata_2024-02.parquet` | |
   | `data/yellow_tripdata_2024-03.parquet` | |

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
