# Benchmark Report: Query Optimization and Platform Evaluation

## 1. Benchmark Methodology
To evaluate the analytical performance of the platform, we compared the analytical queries before and after optimization. For each analytical query, we measured:
* Execution time
* Storage overhead of the analytical data products (where applicable)
* Changes in the query execution plan (using `EXPLAIN FORMATTED`)
* The effect of each optimization technique (Caching, Partition Pruning, Broadcast Joins, and AQE)

To establish baselines, we toggled specific Spark configurations (e.g., disabling automatic broadcasting with `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")` or `spark.sql.adaptive.enabled`). 

We verified that the optimized implementation produces the exact same results as the original implementation using our verification function:
```python
def verify_optimization(baseline_data, optimized_data):
    assert(baseline_data == optimized_data)
```
    

## 2. Execution Times and Optimization Effects

**Partition Pruning**

* **Time:** 4.3 seconds -> 1.0 seconds.
* **Effect:** Organizing the Delta files into `year=XXXX/month=XX` folders has a very minimal storage footprint but massive retrieval benefits.

**Broadcast Joins**

* **Time:** 2.42 seconds -> 0.91 seconds.
* **Effect:** Prevented the huge `trip_data` table from being shuffled across the network by sending the tiny `taxi_zones` table to all workers.

**Adaptive Query Execution (AQE)**

* **Time:** 2.0 seconds -> 0.5 seconds.
* **Effect:** Spark paused, realized the `WHERE fare_amount > 100` filter trimmed the data down, and changed its strategy mid-flight.

**Caching**

* **Time:** 6.0 seconds (Disk) -> 1.0 second (OS Cache) -> 11.0 seconds (Spark Cache Tax) -> 0.58 seconds (Optimized RAM).
* **Effect:** Brings deserialized Java objects entirely into the cluster's RAM, consuming memory resources while active.

---

## 3. Analysis of Query Execution Plans (EXPLAIN FORMATTED)

#### Physical Plan Changes: Broadcast Join

```text
+- * BroadcastHashJoin Inner BuildRight (9)
            :- * Project (4)
            :  +- * Filter (3)
            :     +- * ColumnarToRow (2)
            :        +- Scan parquet  (1)
            +- BroadcastExchange (8)
               +- * Filter (7)
                  +- * ColumnarToRow (6)
                     +- Scan parquet  (5)

```
The broadcasting of the smaller table was done in the execution befor the splitting and sending of the bigger table

#### Physical Plan Changes: Partition Pruning

```text
PartitionFilters: [isnotnull(year#159), isnotnull(month#160), (year#159 = 2024), (month#160 = 1)]
```


#### AQE
```bash
   +- AQEShuffleRead (21)
      +- ShuffleQueryStage (20), Statistics(sizeInBytes=1584.0 B, rowCount=38)
```
The Shuffling was done before the broadcasting, meaning that the data was parsed before being sent 

---

## 4. Storage Overhead of Analytical Data Products

Storage overhead on disk is primarily applicable to our Partitioning strategies. The other optimization techniques happen in the RAM. Hence, for the 2 types of partitioning strategies:

* **Strategy 1 (Partition by Year/Month):** Consumed **452.6 MB** on disk.
* **Strategy 2 (Partition by Year/Month/Day):** Consumed **449.6 MB** on disk.

The storage footprint between the two partitioning strategies is nearly identical (a difference of just 3 MB). But dividing the data brings a massive read boost.

---

## 5. Discussion of Results

**Which optimization produced the largest performance improvement?**
Partition Pruning provided the largest absolute time save (dropping from 4.3s to 1s). By preventing Spark from reading irrelevant data from the hard drive in the first place, we saved a massive amount of I/O operations.

**Which optimization had little or no effect? Why?**
Caching had a surprising effect. The second time we ran a query *without* Spark caching, it still dropped to 1 second! This is because the Operating System (OS Page Cache) secretly kept the files in memory.

**Which queries remain computationally expensive?**
Queries that filter on non-partitioned temporal columns (like `tpep_pickup_datetime` instead of `year` and `month`) remain very expensive because they force a full table scan.

**What characteristics of the data explain these results?**

* **Time-series data:** Taxi trips are chronological. This is why partitioning by `year` and `month` works so nice.
* **Different Size Tables:** The `taxi_zones` dataset is extremely small compared to millions of trips. That is why Broadcast Joins are so effective.
* **Data Skew:** Very few trips cost more than $100. This makes spark realize that it can parse it first and afterwards join.

---

## 6. Recommendations for Expanding to Ten Cities

If the municipality expanded this platform to process data from ten different cities, I would recommend the following changes:

1. **Update the Partitioning Strategy:** We would need to add `city` as a partition key. This would mean `partitionBy("city", "year", "month")`.
2. **AQE Shining:** With ten cities, data skew will be a massive problem. This means that some cities will have hundreds of trips, while others will have millions. This will mean that AQE will be very valuable.
3. **Small Datasets are no Longer Small:** This would mean that sending the `taxi_zone` database to multiple workers will require tuning the bounds of spark (in order to be able to send it).
