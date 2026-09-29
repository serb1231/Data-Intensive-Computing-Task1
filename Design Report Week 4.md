# Design report for week 4

## Task 1. Design the Training Dataset

### The prediction problem

We predict **hourly taxi demand per pickup zone**: at the start of an hour, how many yellow-taxi trips will start in a given taxi zone during that hour. This is the planners' question ("where will demand be in the next hour?"), and it is the only one of the three candidate problems where weather and air quality line up with the target. Both are hourly, citywide measurements, and the target is hourly too. A trip's duration or fare depends mostly on its distance and route, and the fare is computed from distance and time, so most of the useful inputs of those problems are only known once the trip has happened.

The prediction setting is: at the start of hour _h_, the demand of every zone up to hour _h_−1 is known, and so is the weather of hour _h_ (an observed value stands in for a one-hour forecast).

### How the dataset is generated

`python training_dataset.py` builds the dataset in about 27 seconds. Everything it builds is declared in `ml_config.py`: the target, the features, the window, the lags, the splits and the zone selection. The builder reads that configuration and does not change when the configuration does.

1. **Pin the sources.** The builder reads the integrated table, `taxi_zones` and the trip quarantine at fixed Delta versions (`--source-version N` rebuilds from an older integrated table, with the other tables as they were at that commit).
2. **Resolve the window.** It keeps the configured window (January 1 to April 1, 2024), clipped to the months the platform reports on. This reuses `data_products.discover_window`, so the mis-dated trips of 2009-01 and 2023-12 stay out.
3. **Count pickups** per zone and local hour in one pass over the trips. The weather and PM2.5 of each hour are taken in the same pass: the platform joined them on the pickup hour, so every trip of an hour carries the same values. We checked this: no hour has two distinct temperatures or PM2.5 values.
4. **Select the zones** and build the complete grid of selected zones × hours. A zone-hour without a pickup gets `trip_count = 0`.
5. **Add the lags** by joining the zero-filled series to itself, shifted by 1, 24 and 168 hours on the local clock.
6. **Label the splits and write.** It assigns each row to train, validation or test and writes a Delta table (`output_data/ml/hourly_zone_demand`, 5 MB, one file per split, partitioned by `split`). It records what it built in the Delta commit and in `training_dataset_week_4.json`.

The result has **486,809 rows**: 223 zones × 2,183 hours.

| Column | Role | Source |
| --- | --- | --- |
| `pickup_hour` | key; source of the calendar features | integrated table, `tpep_pickup_datetime` truncated to the local hour |
| `pulocationid` | key; the zone as a categorical feature | `taxi_zones.locationid` |
| `trip_count` | **target** | integrated table + trip quarantine: pickups per zone and hour |
| `pickup_borough`, `pickup_service_zone` | categorical features | `taxi_zones` |
| `temp`, `rhum`, `prcp`, `wspd`, `pres` | numeric features | weather, via the integrated table |
| `coco` | categorical feature (condition code) | weather, via the integrated table |
| `pm25` | numeric feature | air quality, via the integrated table |
| `trip_count_lag_1h`, `_24h`, `_168h` | numeric features | the target series itself |
| `split` | train / validation / test | `pickup_hour` |

### How the target variable was defined

`trip_count` is the number of trips whose pickup falls in the zone and the local hour. Three decisions shape it:

- **An empty hour is a zero, not a missing row.** Only 45% of the training zone-hours have a pickup. Dropping the others would leave the model with nothing but hours in which a pickup occurred, and the lags would silently skip hours.
- **The hour is the New York wall-clock hour, independent of Spark's session time zone.** The integrated table's `pickup_hour` (and `date_trunc`) converts through the session time zone, Europe/Stockholm on our machines. Stockholm's clocks jumped on March 31 at 02:00, New York's on March 10, so the trips of 02:00–02:59 on March 31 appear in the 03:00 hour. The builder derives the hour from the fields of the `timestamp_ntz` pickup time instead (`make_timestamp_ntz`), and a test runs it in a Stockholm session to guard this.
- **Trips quarantined only for their passenger count are counted.** The Week 3 validation quarantines a trip whose `passenger_count` is missing or 0. For passenger analytics that is right, but for demand the trip still happened. Leaving these trips out is not a constant factor that a model can absorb:
  - they are 4% of pickups around midday but 26% at 4 am;
  - they grow from 5% of pickups in January to 13% in March, so the gap would widen from the training weeks to the test weeks.

  The builder therefore adds the 731,797 trips whose only rejection reasons are `not_null(passenger_count)` or `positive(passenger_count)`. Each is counted once: the Week 3 update re-delivered 14,608 of them, so the quarantine holds those twice. None of them is also in the integrated table. Trips quarantined for any other reason (zero distance, negative amounts, duplicates) stay out. The rule is one line in `ml_config.py` (`count_quarantined_for`), and an empty value counts validated trips only.

The target is left as a raw count. It is heavily skewed: a mean of 18.0 and a standard deviation of 52.2 in the training weeks, with a maximum of 791. Transforming it (e.g., `log1p`) is a modelling decision for Task 3.

### Which features were included and why

- **Zone identity, borough and service zone** (`taxi_zones`). Where a trip starts is the strongest single determinant of demand: Midtown Center averages 196 pickups per hour in the training weeks, while most outer-borough zones average less than one. The borough and service zone (Yellow Zone, Boro Zone, Airports) let a model share information between similar zones.
- **The pickup hour.** Demand follows a daily and weekly rhythm. The dataset keeps the timestamp; hour of day, day of week, month and holidays are derived from it by the feature pipeline (Task 2).
- **Weather** (`temp`, `rhum`, `prcp`, `wspd`, `pres`, `coco`). Rain, cold and wind are the classic reasons to take a taxi instead of walking.
- **PM2.5.** The question from Week 2 was whether poor air changes travel behaviour.
- **Lags of the target** (1 hour, same hour yesterday, same hour last week). Recent demand is the best evidence of a zone's current level. The lags must be computed on the complete, zero-filled series before the split, which is why they belong to the dataset rather than to the feature pipeline.

Left out, and why:

- **Snow depth** (`snwd`): the station reported none in the quarter (100% NULL).
- **`humidity` and `aqi`**, added by the Week 3 release: they are not in the integrated table, and in the release they are a copy of `rhum` and a rescaled PM2.5. `ml_config.py` notes them for when they carry new information.
- **The zone name**: it carries the same information as `pulocationid`.
- **Dropoff zone, distance, fare, passengers and payment type**: these describe trips that have already happened. A demand count per pickup zone and hour has no dropoff, and none of these values is known in advance.

### Splits

The splits are **chronological, in whole weeks, counted back from the end of the window**:

| Split | Weeks | Days | Rows | Mean `trip_count` | Zero share |
| --- | --- | --- | --- | --- | --- |
| train | 9 | Jan 1 – Mar 3 | 337,176 | 18.0 | 55.4% |
| validation | 2 | Mar 4 – Mar 17 | 74,705 | 21.1 | 50.0% |
| test | 2 | Mar 18 – Mar 31 | 74,928 | 20.5 | 48.4% |

- **Why not a random split?** Neighbouring hours of a zone are nearly the same number. With a random split, the test rows' own neighbours, and their `lag_1h`, would be training targets, and the evaluation would measure interpolation rather than forecasting.
- **Why whole weeks?** January 1, 2024 is a Monday, so the 13 weeks divide exactly. Each held-out split contains every weekday equally often. Since weekday and weekend demand differ strongly, a split of 10 days would favour some weekdays over others.
- **Lags across a boundary** (e.g., the first validation hour's `lag_1h`, a training-period value) are legitimate: at prediction time the past is known.
- **The zones are chosen on the training weeks only.** A zone is in the dataset if it is a place (not 264 "Unknown" or 265 "Outside of NYC") and averaged at least one pickup per day from January 1 to March 3. The held-out weeks thus have no say in the dataset's shape.
- **Retraining** uses the same rule: with a later `window_end`, the newest four weeks become validation and test, and everything before becomes train.

### Which datasets contributed useful information

The builder measures, on the training weeks only, how much each source tells about the target (`signal` in `training_dataset_week_4.json`). The figures are descriptive, not a model:

| Source | What it gives | Measured signal |
| --- | --- | --- |
| Taxi zones | zone identity, borough | the zone's mean explains 58.6% of the target's variance; borough alone 22.3% |
| Trips (timestamp) | the weekly rhythm | hour of the week alone: 3.7%. Zone × hour of the week: 95.8% in sample, and **94.6% of the validation weeks' variance** with the means learned on the training weeks |
| Trips (target history) | lags | r = 0.96 (1 h), 0.92 (24 h), 0.96 (168 h) with the target; r = 0.13, 0.06, 0.11 with what zone × hour of the week leaves unexplained |
| Weather | hourly conditions | beyond zone × hour of the week: \|r\| ≤ 0.045 (pressure 0.045, humidity −0.030, precipitation −0.016, temperature 0.015); the condition code explains 0.4% of the residual |
| Air quality | PM2.5 | r = 0.015 beyond zone × hour of the week |

The trips and the zone lookup carry almost all the information. Demand is multiplicative: a busy zone's peak adds hundreds of trips, a quiet zone's one. So neither the zone nor the hour explains much of the variance on its own, but together they explain 95%. Weather and air quality add little on average in January to March 2024. They are one reading per hour for the whole city, while most of the variance lies between zones. What they can still explain is the residual 5%, e.g. a storm hour. The platform contributed more than the columns themselves:

- the joins that attach weather and air quality to the right hour;
- air quality scoped to New York City monitors;
- validated trips;
- a zone lookup whose keys every trip references.

### Assumptions made during feature construction

- **Weather is known for the predicted hour.** The observed weather stands in for a one-hour forecast, which is accurate at this horizon. A deployed model would use the forecast.
- **One set of conditions for the whole city.** Weather is a single hourly series, and PM2.5 is the hourly average of the monitors in the Bronx, Brooklyn and Queens (Manhattan and Staten Island have none in the extract).
- **An hour exists if a trip started in it anywhere in the city.** 2,183 of the 2,184 hours qualify; the missing one is 02:00 on March 10, when New York's clocks jumped. Lags that point at it are NULL. When clocks go back in November, two real hours will share one local hour; this does not occur in the window.
- **A zero means no pickup, not missing data.** This relies on the platform receiving every trip of an hour.
- **Recorded pickups are demand.** The target counts yellow-taxi trips that happened, not riders who found no taxi, and not green taxis or ride-hailing trips.
- **April 2024 is not demand.** The Week 3 update added 1.68 M April trips, copies of Q1 trips whose pickup times were drawn uniformly over nine days. They have no daily cycle: 65,000 to 77,000 trips at every hour of the day, against 15,000 at 4 am and 213,000 at 6 pm in March. The window therefore ends on April 1 (`window_end`). New real data only needs a later `window_end` or `--end`.
- **Zones with almost no service are out.** 36 zones (18 on Staten Island) had 637 pickups between them in the quarter. Together with "Unknown" and "Outside of NYC" (31,520 pickups) they hold 0.35% of the pickups. The dataset forecasts the other 223 zones only.
- **NULL means unknown.** A lag before the first hour of data is NULL (in the training weeks: 0.07% of `lag_1h`, 1.6% of `lag_24h`, 11.1% of `lag_168h`), as is precipitation where the station did not report it (7.9%). The dataset leaves them NULL; imputing them is the feature pipeline's job (Task 2), fitted on the training weeks.

### Reproducibility and handover to Task 2

- **No sampling and no randomness.** The dataset is a function of the pinned source versions and `ml_config.py`, and both are recorded in the Delta commit and the manifest.
- **Rebuilding from an older version gives the same dataset.** We rebuilt it from version 0 of the integrated table (before the Week 3 update) and obtained exactly the same 486,809 rows, since the update only added April.
- **Tests.** `tests/test_training_dataset.py` covers the zero-filling, the lags, the quarantined pickups, the week-aligned splits, the window and the time-zone behaviour.
- **What Task 2 gets.** `ml_config.py` lists the categorical columns (`pulocationid`, `pickup_borough`, `pickup_service_zone`, `coco`). The manifest's `columns` section describes every column's role and source. The feature pipeline derives the calendar features from `pickup_hour`, encodes the categorical columns, imputes and scales the numeric ones, and fits all of it on the train split only.
