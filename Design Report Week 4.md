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

## Task 2. Build a Reusable Feature Engineering Pipeline

### The pipeline

The input is the Task 1 dataset, which is itself generated from the Week 1 integrated table. `feature_pipeline.py` turns it into one `features` vector per row with a Spark ML `Pipeline` of six stages:

| # | Stage | Spark ML | What it does | Learns from train |
| --- | --- | --- | --- | --- |
| 1 | calendar | `SQLTransformer` | derives `hour_of_day`, `day_of_week`, `month`, `is_weekend` and `is_holiday` (US federal holidays) from `pickup_hour` | – |
| 2 | prepare | `SQLTransformer` | `log1p` of the three lags; a 0/1 `<column>_missing` flag for every quantity that has NULLs in the train split | – |
| 3 | impute | `Imputer` | a missing quantity becomes the train split's median | medians |
| 4 | encode | `StringIndexer` + `OneHotEncoder` | a code becomes one indicator per category; NULL or a category never seen in training becomes all zeros | categories |
| 5 | scale | `VectorAssembler` + `StandardScaler` | quantities to mean 0 and standard deviation 1; indicators and flags are not scaled | means, standard deviations |
| 6 | assemble | `VectorAssembler` | scaled quantities, indicators and flags → `features` (293 slots) | – |

**The plan.** Which column goes through which stage is not written in the code. `plan_features` reads each column's kind from `ml_config.py`:

- `categorical_features` for the dataset's codes and `categorical_calendar` for the derived ones;
- `binary_features` for the flags;
- `log_features` for the skewed counts;
- every other column is a quantity.

It then profiles the train split, and a feature missing in more than half of it, or constant in it, is dropped and reported. `build_feature_pipeline` turns this plan into the stages.

**Fitting.** Every estimator (imputer, indexer, scaler) is fitted on the train split only, so the validation and test weeks influence neither the medians nor the scaling nor the categories. On our data:

- fitting takes 4.6 s, and transforming and writing all 486,809 rows 5.3 s;
- the feature dataset (`output_data/ml/hourly_zone_demand_features`, Delta, partitioned by `split`) takes 10 MB;
- the fitted pipeline (`output_data/ml/models/hourly_zone_demand_features`) takes 156 KB and loads with `PipelineModel.load`.

`feature_pipeline_week_4.json` records the plan, every learned value and the name of every slot of the vector.

**Removing unnecessary attributes** happens at two levels:

- _Features the training data cannot teach are dropped by the plan._ On our data nothing qualifies. A narrower window would drop `month` as constant, and adding snow depth (`snwd`, never reported in Q1) to the dataset would drop it as missing (the tests cover both cases).
- _Columns a model does not need are removed from the feature dataset._ Of the 16 input columns and the 36 intermediate ones, it keeps only the row's keys (`pickup_hour`, `pulocationid`), the target, the split and `features`.

### Why each feature was selected

| Feature | Representation | Slots | Why |
| --- | --- | --- | --- |
| `trip_count_lag_1h`, `_24h`, `_168h` | `log1p`, missing flag, median, scaled | 2 each | the zone's current level of demand, and its value at the same hour yesterday and last week |
| `pulocationid` (pickup zone) | one-hot | 223 | where a trip starts decides the order of magnitude of demand |
| `pickup_borough`, `pickup_service_zone` | one-hot | 5, 4 | let similar zones share what they learn (Manhattan, Yellow Zone, Airports) |
| `hour_of_day`, `day_of_week` | derived, one-hot | 24, 7 | the daily and weekly rhythm; codes, because demand is not a straight line in either (a trough at 4 am, a peak at 6 pm) |
| `is_weekend`, `is_holiday` | derived flags | 1 each | weekend and holiday behaviour in one slot; the holiday list is configuration |
| `month` | derived quantity, scaled | 1 | seasonality and trend; weak with a single quarter, useful once the data spans a year |
| `temp`, `rhum`, `prcp`, `wspd`, `pres` | median, scaled (`prcp` also flagged) | 1 (2) | the conditions of the hour |
| `coco` | one-hot | 14 | the weather condition is a code (rain, snow, fog), not a quantity |
| `pm25` | median, scaled | 1 | air quality, the Week 2 question |

A dropoff zone, the assignment's other location example, does not exist in this problem: a count of pickups per zone and hour has no dropoff (Task 1).

### Which features required the most preprocessing

- **The lags: four steps each.**
  - `log1p`, because they run from 0 to 791 with a mean of 18, and one busy zone would otherwise dominate every distance or gradient.
  - A missing flag, because their first hours have no history (11% of `lag_168h` in the train split).
  - Median imputation: the median of a log-lag is 0, since most zone-hours are quiet, and without the flag a missing week would look like a quiet one.
  - Scaling.
- **Precipitation: three steps.** A flag, imputation (median 0.0 mm) and scaling, because the station did not report it in 7.9% of the training hours.
- **The pickup zone: two steps, but the biggest output.** Indexing and one-hot encoding turn it into 223 of the 293 slots (76%). The indexer's `keep` setting is what lets a zone that appears after training pass through as all zeros instead of failing the job.
- **The calendar features: the most work upstream.** They look cheap here, but `pickup_hour` is only a correct local hour because Task 1 built it independently of Spark's session time zone.

### Which datasets contributed the most valuable features

To answer this with the features themselves, the pipeline fits a probe: a random forest (30 trees, depth 10) on the train split. Its importances are summed per source dataset:

| Source | Importance | Strongest slots |
| --- | --- | --- |
| Trips, demand history (lags) | 84.5% | `lag_1h` 39.4%, `lag_24h` 26.7%, `lag_168h` 18.3% |
| Taxi zones | 13.0% | `service_zone = Yellow Zone` 3.7%, `borough = Manhattan` 2.8%, `service_zone = Boro Zone` 2.4%, `service_zone = Airports` 1.0%, `pulocationid = 161` (Midtown Center) 0.9% |
| Trips, pickup time (calendar) | 1.8% | |
| Weather | 0.6% | |
| Air quality | 0.1% | |

The trips and the zone lookup carry nearly everything, which confirms the Task 1 signal analysis. The calendar looks small only because the lags already contain it: `lag_24h` and `lag_168h` are the same hour yesterday and last week. Tree importances also share credit among correlated features. Weather and PM2.5 are one reading per hour for the whole city, and add little at the zone level.

The probe scores R² 0.930 on the validation weeks (RMSE 15.6, MAE 4.6 trips). It is untuned and only shows that the features can be learned from. It is also below the 0.946 that the zone × hour-of-week average from Task 1 reaches on its own, which is the baseline the Task 3 model has to beat.

What the platform contributed is less visible but decisive:

- the trips behind the lags are validated and deduplicated;
- every zone ID references the lookup;
- weather and air quality arrive already aligned to the hour.

### How the pipeline supports future extensions

- **A new feature is a line of configuration.** Once the platform integrates a source (Weeks 1–3: declared schema, validation, join), its column is listed in `ml_config.py` (`hourly_features` or `zone_features`). If it is a code or a skewed count, it is also listed as categorical or in `log_features`. The plan routes it to the right stages; `test_a_new_feature_is_a_line_of_configuration` checks this. A calendar feature is one SQL expression in `calendar_features`.
- **Unusable features do not break the pipeline.** A column that is empty or constant in the training window (such as `snwd` in Q1 2024) is dropped and reported, instead of crashing the imputer or wasting a slot.
- **New categories at prediction time.** A zone or weather code never seen in training encodes as all zeros. After retraining it gets its own slot. Categories are ordered alphabetically, not by frequency, so a slot does not move when a zone becomes busier.
- **Models plug in behind it.** Every stage is a standard Spark ML stage, so the fitted pipeline saves and loads without custom code (tested). Task 3 appends any MLlib regressor to `feature_pipeline(train)` and fits both together.
- **Retraining refits the same stages on a new train split.** The plan is re-profiled, and the medians, scaling and categories are relearned; no code changes.
- **A new kind of transformation is one stage in `build_feature_pipeline`.** The most promising one is a zone × hour-of-week average learned on the train split (target encoding), given that it alone explains 94.6% of the validation variance in Task 1.
- **Other prediction problems** get their own pair of `TrainingDatasetConfig` and `FeaturePipelineConfig`. The code names only the keys `pickup_hour` and `pulocationid`.

### Trade-offs

- **One-hot encoding of 223 zones** gives a sparse vector of 293 slots, which is fine for linear models and trees at this size. With thousands of categories we would switch to target or hash encoding.
- **Fitting only on the train split** keeps the validation and test measurements honest. Once a model is chosen, refitting on train and validation together is a decision for Task 3.
- **The probe adds about 70 seconds** to a run of 20. `--no-probe` skips it when only the features are needed.

## Task 3. Build a Reproducible ML Pipeline

### The pipeline

`train_model.py` is the whole of Task 3. It reads the current Delta version of the training dataset (Task 1), asks `feature_pipeline()` for the fitted-
on-demand feature stages (Task 2), appends one regressor, and fits the two together.

```python
features, plan = feature_pipeline(train)
regressor = GBTRegressor(featuresCol=FEATURES, labelCol=target, maxIter=50, maxDepth=5, seed=42)
model = Pipeline(stages=features.getStages() + [regressor]).fit(train)
```

For the model, we went with a Gradient Boosted Tree. Since we're trying to predict demand (which is just a count), 
trees handle this really well, especially when splitting up the data by hour and location. We made sure to only train  
it on the training set.

### Model evaluation

Scored on the three splits of the same build (`model_week_4.json`, dataset v20):

| Split | Weeks | RMSE | MAE | R² |
| --- | --- | --- | --- | --- |
| train | 9 | 12.08 | 4.00 | 0.948 |
| validation | 2 | 14.70 | 4.71 | 0.940 |
| test | 2 | 14.61 | 4.73 | 0.936 |

The scores for the training, validation, and test sets all ended up being pretty close. This means the model isn't 
overfitting—it actually works well on future data it hasn't seen before.

Our average error was only about 4.7 pickups per hour. That's a really good result considering some zones get hundreds 
of pickups in a single hour.

### Which parts of the pipeline are reusable?

The dataset builder, the feature stages, the plan and the split
boundaries come from Tasks 1 and 2 unchanged; `train_model.py` contributes the regressor, the
metrics and the manifest.

### How can new features be incorporated?

A new feature changes the size of the vector and nothing  else. Features that are empty or constant in the train split
are still dropped by the plan and reported in `dropped_features`.

### How is retraining supported?

Retraining is running the same two commands again: `python training_dataset.py` rebuilds the
dataset from the current Delta versions of the platform tables, and `python train_model.py` refits
and overwrites the model.

### How could the pipeline support multiple prediction tasks?

A second prediction problem (trip duration, fare amount) is a second pair of `TrainingDatasetConfig`
and `FeaturePipelineConfig` in `ml_config.py`: the target, its features and its splits. The three
scripts read the configuration they are given, and the only column names in the code are the keys
`pickup_hour` and `pulocationid`.

### Trade-offs

- **One model:** 50 trees of depth 5 with a fixed seed train in about 90 seconds. Tuning on the validation split would improve the numbers
- **Predictions can be negative** the test rows show about -0.008 for an empty zone-hour
