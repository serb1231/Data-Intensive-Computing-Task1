import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)

def create_data_taxi_trips() -> tuple[int, int]:
    tripdata_1 = pd.read_parquet('data/yellow_tripdata_2024-01.parquet', engine='fastparquet')
    tripdata_2 = pd.read_parquet('data/yellow_tripdata_2024-02.parquet', engine='fastparquet')
    tripdata_3 = pd.read_parquet('data/yellow_tripdata_2024-03.parquet', engine='fastparquet')

    tripdata_merged = pd.concat([tripdata_1, tripdata_2, tripdata_3], axis=0)
    # subset columns to get duplicates
    columns_to_test_duplication = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance',
                                   'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type']

    print(len(tripdata_merged))
    tripdata_merged_duplicates_sorted = tripdata_merged[
        tripdata_merged.duplicated(subset=columns_to_test_duplication, keep=False)
    ].sort_values(columns_to_test_duplication)
    # print(len(tripdata_merged_duplicates_sorted))
    tripdata_merged_no_duplicates = tripdata_merged[~tripdata_merged.duplicated(subset=columns_to_test_duplication, keep=False)]
    tripdata_merged_no_duplicates = tripdata_merged_no_duplicates[tripdata_merged_no_duplicates['total_amount'] > 0]

    print(len(tripdata_merged_no_duplicates))

    # grab 10% of data. Use every 5th row as we got rid of half the data by getting the positive total_amount
    indices_to_copy = list(range(0, len(tripdata_merged_no_duplicates), 5))
    new_trip_data = tripdata_merged_no_duplicates.iloc[indices_to_copy].copy()

    # find the latest timestamp in original data
    last_event_time = tripdata_merged_no_duplicates['tpep_dropoff_datetime'].max()

    # compute original location and randomize it slightly
    original_durations = new_trip_data['tpep_dropoff_datetime'] - new_trip_data['tpep_pickup_datetime']
    duration_multipliers = np.random.uniform(0.8, 1.2, size=len(new_trip_data))
    new_durations = original_durations * duration_multipliers

    # generate random offsets after the last event time
    random_offsets_seconds = np.random.randint(3600, 60 * 60 * 24 * 7, size=len(new_trip_data))
    random_offsets_seconds.sort()

    # create the new pickup and dropoff times based on the last event time and random offsets
    new_pickups = last_event_time + pd.to_timedelta(random_offsets_seconds, unit='s')
    new_dropoffs = new_pickups + new_durations

    new_trip_data['tpep_pickup_datetime'] = new_pickups
    new_trip_data['tpep_dropoff_datetime'] = new_dropoffs

    # copy 2% or the original data to the new data
    indices_to_copy_2_percent = list(range(0, len(tripdata_merged_no_duplicates), 50))
    new_trip_data_2_percent = tripdata_merged_no_duplicates.iloc[indices_to_copy_2_percent].copy()

    # REPLACED .append() WITH pd.concat() DUE TO PANDAS 2.0 DEPRECATION
    new_trip_data = pd.concat([new_trip_data, new_trip_data_2_percent])

    # sort the data
    new_trip_data = new_trip_data.sort_values(by=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

    # save it to a parquet file inside the continuous_data directory
    new_trip_data.to_parquet(
        'continuous_data/tripdata_merged_no_duplicates_continuous_sorted.parquet',
        engine='pyarrow', index=False,
        coerce_timestamps='us', allow_truncated_timestamps=True,
    )
    new_trip_data.to_csv('.tmp/tripdata_merged_no_duplicates_continuous_sorted.csv', index=False)

    duplicated_count = new_trip_data.duplicated(subset=columns_to_test_duplication, keep=False).sum()
    return len(new_trip_data), duplicated_count


def create_weather_data_continuous() -> int:
    weatherdata = pd.read_csv('data/weather.csv')

    # sort the data by year, month, day, hour
    weatherdata = weatherdata.sort_values(by=['year', 'month', 'day', 'hour'])
    # make it so that the year is one higher than the last year in the original data
    last_year = weatherdata['year'].max()
    weatherdata['year'] = weatherdata['year'] + 1

    # 2025 has no Feb 29: keep only (year, month, day) combinations that are real dates
    valid_date = pd.to_datetime(weatherdata[['year', 'month', 'day']], errors='coerce').notna()
    weatherdata = weatherdata[valid_date]

    # create a column humidity that is based on rhum
    weatherdata['humidity'] = weatherdata['rhum']

    # save it to a csv file inside the continuous_data directory
    weatherdata.to_csv('continuous_data/weather_continuous.csv', index=False)

    return len(weatherdata)


def shift_to_year(s: pd.Series, target_year: int) -> pd.Series:
    """Shift every date in s to target_year, keeping month/day (same behavior as DateOffset)."""
    out = s.copy()
    years = s.dt.year
    for y in years.unique():
        mask = years == y
        out[mask] = s[mask] + pd.DateOffset(years=target_year - int(y))
    return out


def create_air_quality_data_continuous() -> int:
    airQuality = pd.read_csv(
        'data/air_quality.csv', low_memory=False,
        dtype={"State Code": str, "County Code": str, "Site Num": str,
               "Parameter Code": str, "POC": str},
    )

    date_cols = ["Date GMT", "Date Local", "Date of Last Change"]
    for c in date_cols:
        airQuality[c] = pd.to_datetime(airQuality[c], format="%Y-%m-%d")

    # Feb 29 has no equivalent in 2025; DateOffset would clip it onto Feb 28
    for c in ["Date GMT", "Date Local"]:
        is_feb29 = (airQuality[c].dt.month == 2) & (airQuality[c].dt.day == 29)
        airQuality = airQuality[~is_feb29]

    for c in date_cols:
        airQuality[c] = airQuality[c] + pd.DateOffset(years=1)

    # aqi: vectorized instead of .apply
    sm = airQuality["Sample Measurement"]
    lowest, highest = sm.min(), sm.max()
    airQuality["aqi"] = (sm - lowest) / (highest - lowest) * 100

    airQuality.to_csv('continuous_data/air_quality_continuous.csv', index=False)
    return len(airQuality)


def main():
    nr_taxi_trips, nr_taxi_trips_duplicates = create_data_taxi_trips()
    print(f"Number of taxi trips created: {nr_taxi_trips}")
    print(f"Number of taxi trips duplicates created: {nr_taxi_trips_duplicates}")
    print(f"Number of weather records created: {create_weather_data_continuous()}")
    print(f"Number of air quality records created: {create_air_quality_data_continuous()}")


if __name__ == "__main__":
    main()