from pyspark.sql.types import  StructType, StructField, IntegerType, LongType, DoubleType, StringType, TimestampType, DateType

trips_schema = StructType([
StructField("ride_id", LongType(), True), # originally an unnamed first column in CSV
StructField("VendorID", IntegerType(), True),
StructField("tpep_pickup_datetime", TimestampType(), True),
StructField("tpep_dropoff_datetime", TimestampType(), True),
StructField("passenger_count", IntegerType(), True),
StructField("trip_distance", DoubleType(), True),
StructField("RatecodeID", IntegerType(), True),
StructField("store_and_fwd_flag", StringType(), True),
StructField("PULocationID", IntegerType(), True),
StructField("DOLocationID", IntegerType(), True),
StructField("payment_type", IntegerType(), True),
StructField("fare_amount", DoubleType(), True),
StructField("extra", DoubleType(), True),
StructField("mta_tax", DoubleType(), True),
StructField("tip_amount", DoubleType(), True),
StructField("tolls_amount", DoubleType(), True),
StructField("improvement_surcharge", DoubleType(), True),
StructField("total_amount", DoubleType(), True),
StructField("congestion_surcharge", DoubleType(), True),
StructField("Airport_fee", DoubleType(), True)
])

taxi_zones_schema = StructType([
StructField("LocationID", IntegerType(), True),
StructField("Borough", StringType(), True),
StructField("Zone", StringType(), True),
StructField("service_zone", StringType(), True)
])

weather_schema = StructType([
StructField("year", IntegerType(), True),
StructField("month", IntegerType(), True),
StructField("day", IntegerType(), True),
StructField("hour", IntegerType(), True),
StructField("temp", DoubleType(), True),
StructField("temp_source", StringType(), True),
StructField("rhum", IntegerType(), True), # relative humidity (percent)
StructField("rhum_source", StringType(), True),
StructField("prcp", DoubleType(), True), # precipitation
StructField("prcp_source", StringType(), True),
StructField("snwd", DoubleType(), True), # snow depth
StructField("snwd_source", StringType(), True),
StructField("wdir", IntegerType(), True), # wind direction (deg)
StructField("wdir_source", StringType(), True),
StructField("wspd", DoubleType(), True), # wind speed
StructField("wspd_source", StringType(), True),
StructField("wpgt", DoubleType(), True), # wind gust
StructField("wpgt_source", StringType(), True),
StructField("pres", DoubleType(), True), # pressure
StructField("pres_source", StringType(), True),
StructField("cldc", IntegerType(), True), # cloud cover (code/percent)
StructField("cldc_source", StringType(), True),
StructField("coco", IntegerType(), True), # weather condition code
StructField("coco_source", StringType(), True)
])

air_quality_schema = StructType([
StructField("State Code", StringType(), True), # keep as string to preserve leading zeros like "01"
StructField("County Code", StringType(), True), # preserve leading zeros
StructField("Site Num", StringType(), True),
StructField("Parameter Code", IntegerType(), True),
StructField("POC", IntegerType(), True),
StructField("Latitude", DoubleType(), True),
StructField("Longitude", DoubleType(), True),
StructField("Datum", StringType(), True),
StructField("Parameter Name", StringType(), True),
StructField("Date Local", DateType(), True), # format: YYYY-MM-DD
StructField("Time Local", StringType(), True),
StructField("Date GMT", DateType(), True),
StructField("Time GMT", StringType(), True),
StructField("Sample Measurement", DoubleType(), True),
StructField("Units of Measure", StringType(), True),
StructField("MDL", DoubleType(), True),
StructField("Uncertainty", DoubleType(), True),
StructField("Qualifier", StringType(), True),
StructField("Method Type", StringType(), True),
StructField("Method Code", StringType(), True), # kept as string to be safe (codes sometimes non-numeric)
StructField("Method Name", StringType(), True),
StructField("State Name", StringType(), True),
StructField("County Name", StringType(), True),
StructField("Date of Last Change", DateType(), True)
])