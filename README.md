# NYC Taxi Data Integration & Ingestion Framework

This repository contains a PySpark-based data ingestion and integration pipeline. It processes heterogeneous datasets (NYC Taxi Trips, Weather, Air Quality, and Taxi Zones), standardizes them into a Common Data Model, applies data quality checks, and stores them as partitioned Delta Lake tables.

## 1. Prerequisites

To run this pipeline, you must have the following installed on your machine:

- **Java 8 or 11** (Required for Apache Spark)
- **Conda** (Miniconda or Anaconda)

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
4. Copy insode the data folder the datasets

5. Run the script:
   ```bash
   python data_ingestion.py
   ```
