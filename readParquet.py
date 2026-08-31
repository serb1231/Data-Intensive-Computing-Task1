import pandas as pd
pd.set_option('display.max_columns', None)
tripdata_1 = pd.read_parquet('data/yellow_tripdata_2024-01.parquet', engine='fastparquet')
tripdata_2 = pd.read_parquet('data/yellow_tripdata_2024-02.parquet', engine='fastparquet')
tripdata_3 = pd.read_parquet('data/yellow_tripdata_2024-03.parquet', engine='fastparquet')

tripdata_merged = pd.concat([tripdata_1, tripdata_2, tripdata_3], axis=0)
tripdata_pk = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
tripdata_merged_duplicates_sorted = tripdata_merged[tripdata_merged.duplicated(subset=tripdata_pk, keep=False)].sort_values(tripdata_pk)
tripdata_merged_duplicates_sorted.to_csv('.tmp/tripdata_merged_duplicates_sorted.csv')

weatherdata = pd.read_csv('data/weather.csv')
weatherdata[weatherdata.duplicated(subset=['year','month','day','hour'])].to_csv("weather_data_duplicates.csv")

airQuality = pd.read_csv('data/air_quality.csv', low_memory=False)
airQualityPK = ['State Code', 'County Code', 'Parameter Code', 'Site Num','Date GMT', 'Time GMT', 'Method Name', 'POC']
duplicates = airQuality[airQuality.duplicated(subset=airQualityPK, keep=False)]
sorted_duplicates = duplicates.sort_values(by=airQualityPK)
sorted_duplicates.to_csv('.tmp/airQualityDuplicates.csv')