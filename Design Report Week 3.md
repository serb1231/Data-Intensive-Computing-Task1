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
