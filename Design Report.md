## Task 1. Study the Data

### TripData
*   **What is the primary entity represented by the dataset?**
    Individual taxi trips.
*   **Which attributes uniquely identify a record (i.e., what is the primary key of the dataset)?**
    `tpep_pickup_datetime` and `tpep_dropoff_datetime`. There are duplicates of these, all of which have a positive or negative sum (hinting at a reimbursement from the company). It may be necessary to create a surrogate key (a new key based on a hash).
*   **Which attributes are likely to be used for joins?**
    `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, and `DOLocationID`. The time and date are used to join with the Weather and Taxi Zone Lookup datasets.
*   **Which attributes are temporal?**
    `tpep_pickup_datetime` and `tpep_dropoff_datetime`.
*   **Which attributes contain categorical values?**
    `VendorID`, `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `PULocationID`, `DOLocationID`, and `payment_type`.
*   **Which attributes are likely to grow over time?**
    None; the database grows vertically, not horizontally.

---

### Weather
*   **What is the primary entity represented by the dataset?**
    Hourly weather conditions (snow, wind, rain, temperature).
*   **Which attributes uniquely identify a record (i.e., what is the primary key of the dataset)?**
    `year`, `month`, `day`, and `hour`.
*   **Which attributes are likely to be used for joins?**
    `year`, `month`, `day`, and `hour`.
*   **Which attributes are temporal?**
    `year`, `month`, `day`, and `hour`.
*   **Which attributes contain categorical values?**
    `temp_source`, `rhum`, `rhum_source`, `prcp_source`, `snwd_source`, `wdir`, `wdir_source`, `wspd_source`, `wpgt_source`, `pres_source`, `cldc`, `cldc_source`, `coco`, and `coco_source`.
*   **Which attributes are likely to grow over time?**
    None; the database grows vertically, not horizontally.

---

### Air Quality
*   **What is the primary entity represented by the dataset?**
    Air quality based on location and time.
*   **Which attributes uniquely identify a record (i.e., what is the primary key of the dataset)?**
    `State Code`, `County Code`, `Parameter Code`, `Site Num`, `Date GMT`, `Time GMT`, `Method Name`, and `POC`.
*   **Which attributes are likely to be used for joins?**
    `State Code`, `County Code`, `Date GMT`, and `Time GMT`.
*   **Which attributes are temporal?**
    `Date Local`, `Time Local`, `Date GMT`, and `Time GMT`.
*   **Which attributes contain categorical values?**
    `State Code`, `County Code`, `Parameter Code`, `Site Num`, `POC`, `Datum`, `Parameter Name`, `Units of Measure`, `MDL`, `Method Type`, `Method Code`, `Method Name`, `State Name`, and `County Name`.
*   **Which attributes are likely to grow over time?**
    None; the database grows vertically, not horizontally.

---

### Taxi Zone Lookup
*   **What is the primary entity represented by the dataset?**
    Locations of taxi zones.
*   **Which attributes uniquely identify a record (i.e., what is the primary key of the dataset)?**
    `LocationID`.
*   **Which attributes are likely to be used for joins?**
    `Borough` and `Zone`.
*   **Which attributes are temporal?**
    N/A
*   **Which attributes contain categorical values?**
    `Borough`, `Zone`, and `service_zone`.
*   **Which attributes are likely to grow over time?**
    N/A

## Task 2. Design Your Storage Architecture
### Storage Architecture Design

**1. Directory Structure**
*   `raw_data/`: Used for storing the initial, unprocessed files.
*   `clean_data/`: Used for storing the processed and cleaned data (Delta tables).
*   `.tmp/`: Used for temporary storage during intermediate pipeline steps.

**2. Delta Table Organization & Naming Conventions**
*Naming Convention:* All tables and columns must be lowercase, with words separated by underscores (`_`).
*   `yellow_tripdata_2024-01`, `2024-02`, `2024-03` $\rightarrow$ `taxi_trip`
*   `taxi_zone_lookup.csv` $\rightarrow$ `taxi_zone`
*   `air_quality.csv` $\rightarrow$ `air_quality`
*   `weather.csv` $\rightarrow$ `weather`

**3. Partitioning Strategy**
*   **`taxi_trip`**: Partition by date (year and month).
*   **`weather`**: Partition by year, month, and day.
*   **`air_quality`**: Partition by `state_code`, `county_code`, `parameter_code`, `site_num`, and `date_gmt`.
*   **`taxi_zone`**: Should not be partitioned. It it too small and there is no key to partition after

---

### Design Decisions

**Which datasets should conceptually be treated as lookup tables?**
The `taxi_zone`, `air_quality`, and `weather` datasets should all be treated as lookup tables.

**Which datasets should not be partitioned? Explain why.**
The `taxi_zone` dataset should not be partitioned. It is too small, and partitioning it would make spark slower (opening many folders to find few items)

**Which datasets require different partitioning strategies?**
*   **`taxi_trip`** should be partitioned temporally by year and month (although this has to be tested)
*   **`air_quality`** should be partitioned by location: `state_code`, `county_code`, `parameter_code`, and `site_num`.

**Under what conditions does partitioning become harmful?**
Partitioning becomes harmful when there are too many unique values in the partition column. There must be a balance between the number of folders generated and the amount of data inside each folder.

**If the total data volume increased by 20×, what changes would you make to your storage design?**
Make the partitioning strategy more in depth
*   Partition **`taxi_trip`**: use the day
*   Partition **`air_quality`**: use the date
*   Adjust **`weather`** partition by year (maybe even month)

## Task 3. Build a Generic Ingestion Framework

### Which components are generic and reusable across all datasets?
The date and time columns are generic. The location, after some processing, can be generic and reusable across all datasets.

### Which components remain dataset-specific, and why?
*   **For air quality:** Datum, parameter name, units of measure, and method name.
*   **For the trip data:** The rating and fare amounts.
*   **For the taxi_zone_lookup:** Service zone.
*   **For the weather:** Sources of temperature, humidity, wind, etc.

### How are transformation rules defined and maintained?
*   **General rules:** `readDataGeneric` and `modify_data_generic`, which are reusable for CSV and Parquet files. The modification of the data is done in order to drop rows with bad values (PK is null, the value of a column is negative although it should be positive, timestamps are not well formatted). At the same time, keeping the naming convention to `snake_case`.
*   **Dataset-specific rules:** They create the surrogate key and they modify the date and time defined in each dataframe in order to generalize it inside a new field.
*   **Maintaining:** Whenever a new dataset is added, generic rules are applied first, then a user needs to define the new dataset-specific rules. In order to be compatible, they would define a datetime column and a PK column. The types of data for each dataset are defined in the schema. If a new column is added, or a type is modified, then the schema needs to be modified as well.

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
We decided on the snake format for database columns (hence modified all the names to lowercase, with "_" instead of " ").

**Rules for handling missing values**
For the weather missing values (primarily in the snow category), the schema allows having null values.
Whenever the timestamp data or the primary key data was missing, we decided to eliminate the row as it could not correlate to the other datasets.
For the taxi trips, we eliminated the negative fare amounts.

**Common data types**
The data types are defined in the schema for each dataset. Hence, all datasets have common data types (Long, Integer, String, Double, Timestamp).

**Document every transformation.**
See section "How are transformation rules defined and maintained?" for more details.