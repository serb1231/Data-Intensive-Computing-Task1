"""Fault-injection tests for the validation framework (Task 4) and the monitoring tables (Task 3).

Each test feeds the real pipeline code a small batch with known problems and checks that
every problem is detected, isolated and reported, and that nothing else is.
Run with:  python -m pytest tests
"""
from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql.types import (DateType, DoubleType, IntegerType, LongType, StringType, StructField,
                               StructType, TimestampType)

import monitoring
import validation
import validation_rules
from data_ingestion import execute_pipeline, readDataGeneric, trip_data_process, weather_data_process
from schemas import weather_schema
from validation import (CAST, DROP_COLUMN, EVOLVE, FILL_NULL, MALFORMED_COL, REJECT_BATCH, WARN, DatasetContract,
                        InRange, NotNull, Positive, ReferenceExists, Unique, ValidationContext,
                        check_source_schema, check_target_schema, conform_types, validate)

WEATHER_HEADER = ("year,month,day,hour,temp,temp_source,rhum,rhum_source,prcp,prcp_source,snwd,snwd_source,"
                  "wdir,wdir_source,wspd,wspd_source,wpgt,wpgt_source,pres,pres_source,cldc,cldc_source,"
                  "coco,coco_source")


def weather_line(day, hour, temp="5.0", coco="3", extra=()):
    return ",".join(["2025", "1", str(day), str(hour), temp, "isd", "50", "isd", "0.0", "isd", "", "",
                     "180", "isd", "10.0", "isd", "", "", "1015.0", "isd", "4", "isd", coco, "isd", *extra])


def load_weather(spark, path, target):
    return execute_pipeline("Weather", readDataGeneric(spark, str(path), "csv", weather_schema),
                            weather_data_process, target, pk_col="timestamp",
                            partition_cols=["year", "month", "day"])


# --- reading --------------------------------------------------------------------------

def test_conform_types_accepts_lossless_values_and_flags_the_rest(spark):
    df = spark.createDataFrame([("3.0", "1.5", "2024-01-02"),
                                ("12.5", "abc", "2024-13-01"),
                                (None, None, None)], "i string, d string, dt string")
    rows = conform_types(df, {"i": IntegerType(), "d": DoubleType(), "dt": DateType()}).collect()

    assert rows[0]["i"] == 3 and rows[0][MALFORMED_COL] is None     # pandas-style "3.0" is an integer
    assert rows[1][MALFORMED_COL] == "i=12.5; d=abc; dt=2024-13-01"  # truncation and parse failures
    assert rows[2][MALFORMED_COL] is None                           # missing is not malformed


def test_csv_reader_binds_columns_by_header_name(spark, tmp_path):
    path = tmp_path / "reordered.csv"
    path.write_text("hour,day,month,year,temp,surprise\n5,2,1,2025,-3.5,x\n")
    df = readDataGeneric(spark, str(path), "csv", weather_schema)

    row = df.first()
    assert (row.year, row.month, row.day, row.hour, row.temp) == (2025, 1, 2, 5, -3.5)
    assert dict(df.dtypes)["surprise"] == "string"


# --- record-level rules ---------------------------------------------------------------------

def test_rules_detect_every_problem_category(spark, tmp_path):
    zones = str(tmp_path / "zones")
    spark.createDataFrame([(1,), (2,)], "locationid int").write.format("delta").save(zones)
    df = spark.createDataFrame([
        ("a", 1, 1.0, 1),     # valid
        ("a", 1, 1.0, 1),     # duplicate of the row above
        ("b", None, 1.0, 1),  # incomplete
        ("c", 1, -2.0, 1),    # invalid value
        ("d", 1, 1.0, 99),    # missing reference
        ("e", 1, 900.0, 2),   # implausible but only a warning
    ], "key string, passengers int, distance double, zone int")
    rules = [NotNull("key"), Unique("key"), NotNull("passengers"), Positive("distance"),
             ReferenceExists("zone", zones, "locationid"),
             InRange("distance", max=500, action=WARN),
             InRange("humidity", 0, 100)]  # column not in this batch

    outcome = validate(df, rules, ValidationContext(spark, "test", str(tmp_path / "target"), False))
    results = {r.rule_name: r for r in outcome.results}

    assert results["unique(key)"].failed_records == 1
    assert results["not_null(passengers)"].failed_records == 1
    assert results["positive(distance)"].failed_records == 1
    assert results["reference(zone->zones.locationid)"].failed_records == 1
    assert results["in_range(distance,,500)"].status == "warning"
    assert results["in_range(humidity,0,100)"].status == "skipped"
    assert (outcome.total, outcome.quarantined, outcome.dropped, outcome.warned) == (6, 4, 0, 1)
    assert sorted(r.key for r in outcome.valid().collect()) == ["a", "e"]
    assert set(outcome.valid().columns) == {"key", "passengers", "distance", "zone"}


def test_unique_keeps_the_copy_that_passes_the_row_rules(spark, tmp_path):
    df = spark.createDataFrame([("a", 0), ("a", 3)], "key string, passengers int")
    outcome = validate(df, [Unique("key"), Positive("passengers")],
                       ValidationContext(spark, "test", str(tmp_path / "target"), False))

    assert [r.passengers for r in outcome.valid().collect()] == [3]


# --- dataset-level schema checks -------------------------------------------------------------

def test_schema_checks_classify_changes():
    contract = DatasetContract(
        schema=StructType([StructField("a", IntegerType()), StructField("b", StringType()),
                           StructField("humidity", IntegerType())]),
        required_columns=["a"])
    source = StructType([StructField("b", StringType()), StructField("surprise", StringType())])
    assert {(c.kind, c.column): c.action for c in check_source_schema(contract, source)} == {
        ("missing_required_column", "a"): REJECT_BATCH,
        ("unexpected_column", "surprise"): DROP_COLUMN,
    }

    target = StructType([StructField("a", LongType()), StructField("b", StringType()),
                         StructField("t", TimestampType())])
    batch = StructType([StructField("a", DoubleType()), StructField("t", StringType()),
                        StructField("humidity", IntegerType())])
    assert {(c.kind, c.column): c.action for c in check_target_schema(batch, target)} == {
        ("type_changed", "a"): CAST,
        ("incompatible_type", "t"): REJECT_BATCH,
        ("column_added", "humidity"): EVOLVE,
        ("column_missing", "b"): FILL_NULL,
    }


# --- the whole pipeline ------------------------------------------------------------------------

def test_incremental_weather_load_isolates_and_reports_every_fault(spark, platform):
    target = str(platform / "weather")
    first = platform / "weather_v1.csv"
    first.write_text(WEATHER_HEADER + "\n" + "\n".join(weather_line(1, h) for h in range(3)) + "\n")
    _, meta = load_weather(spark, first, target)
    assert (meta["inserted_records"], meta["rejected_records"]) == (3, 0)

    update = platform / "weather_v2.csv"
    update.write_text(WEATHER_HEADER + ",humidity,surprise\n" + "\n".join([
        weather_line(1, 0, extra=["55", "x"]),              # already loaded -> dropped
        weather_line(2, 0, coco="3.0", extra=["55", "x"]),  # new and valid ("3.0" is a lossless int)
        weather_line(2, 0, coco="3.0", extra=["55", "x"]),  # duplicate within the batch
        weather_line(2, 1, temp="85.0", extra=["55", "x"]),  # implausible temperature
        weather_line(2, 2, temp="", extra=["55", "x"]),      # incomplete
        weather_line(2, 3, coco="abc", extra=["55", "x"]),   # malformed
        weather_line(2, 4, extra=["140", "x"]),              # new column, out of range
    ]) + "\n")
    clean, meta = load_weather(spark, update, target)

    assert meta["status"] == "succeeded"
    # the returned batch must not change once the merge has committed: downstream steps (the
    # integrated table) are built from it, and the not_already_loaded rule reads the target table
    assert clean.count() == meta["inserted_records"] == 1
    assert (meta["processed_records"], meta["inserted_records"],
            meta["dropped_records"], meta["quarantined_records"]) == (7, 1, 1, 5)

    table = spark.read.format("delta").load(target)
    assert table.count() == 4
    assert "humidity" in table.columns                   # declared column: the table evolved
    assert "surprise" not in table.columns               # undeclared column: dropped
    assert MALFORMED_COL not in table.columns

    quarantine = spark.read.format("delta").load(validation.quarantine_path("Weather"))
    reasons = sorted(reason for row in quarantine.collect() for reason in row._rejection_reasons)
    assert reasons == sorted(["unique(timestamp)", "in_range(temp,-40,50)", "not_null(temp)",
                              "well_formed", "in_range(humidity,0,100)"])
    assert quarantine.filter(f"{MALFORMED_COL} = 'coco=abc'").count() == 1

    runs = spark.read.format("delta").load(monitoring.table_path("pipeline_runs")).orderBy("started_at").collect()
    assert [r.schema_version for r in runs] == [1, 2]
    assert "column_added(humidity)" in runs[1].schema_changes
    assert "unexpected_column(surprise)" in runs[1].schema_changes
    checks = spark.read.format("delta").load(monitoring.table_path("validation_results"))
    assert checks.filter(f"execution_id = '{runs[1].execution_id}' AND status = 'failed'").count() \
        == runs[1].validation_failures

    # every operational query runs against what was recorded
    assert monitoring.register_views(spark)
    for _, sql in monitoring.MONITORING_QUERIES.values():
        spark.sql(sql).collect()


def test_batch_missing_a_required_column_is_rejected_and_nothing_is_written(spark, platform):
    path = platform / "no_temp.csv"
    path.write_text("year,month,day,hour\n2025,1,1,0\n")
    target = str(platform / "weather")

    clean, meta = load_weather(spark, path, target)

    assert (meta["status"], meta["rejected_records"], clean.count()) == ("rejected", 1, 0)
    assert not DeltaTable.isDeltaTable(spark, target)
    run = spark.read.format("delta").load(monitoring.table_path("pipeline_runs")).first()
    assert (run.status, run.validation_failures) == ("rejected", 1)


def test_trip_type_change_is_cast_back_and_lossy_values_are_flagged(spark, platform, monkeypatch):
    zones = str(platform / "zones")
    spark.createDataFrame([(1,), (2,)], "locationid int").write.format("delta").save(zones)
    base = validation_rules.CONTRACTS["Trip Data"]
    rules = [ReferenceExists(r.column, zones, r.ref_column) if isinstance(r, ReferenceExists) else r
             for r in base.rules]
    monkeypatch.setitem(validation_rules.CONTRACTS, "Trip Data",
                        DatasetContract(schema=base.schema, required_columns=base.required_columns, rules=rules))

    def trips(rows, passenger_type):
        schema = (f"vendorid int, tpep_pickup_datetime timestamp_ntz, tpep_dropoff_datetime timestamp_ntz, "
                  f"passenger_count {passenger_type}, trip_distance double, ratecodeid bigint, "
                  f"store_and_fwd_flag string, pulocationid int, dolocationid int, payment_type bigint, "
                  f"fare_amount double, total_amount double")
        pickup = lambda hour: datetime(2024, 4, 1, hour, 30)
        return spark.createDataFrame([(2, pickup(h), pickup(h) + timedelta(minutes=minutes),
                                       passengers, 2.5, 1, "N", zone, 1, 1, 12.0, 15.0)
                                      for h, minutes, passengers, zone in rows], schema)

    target = str(platform / "trips")
    _, meta = execute_pipeline("Trip Data", trips([(1, 20, 1, 1), (2, 20, 2, 2)], "bigint"), trip_data_process,
                               target, pk_col="surrogate_key", partition_cols=["year", "month"])
    assert meta["inserted_records"] == 2

    update = trips([(3, 20, 1.0, 1),    # valid, 1.0 casts back to 1
                    (4, 20, 1.5, 1),    # 1.5 passengers cannot be a bigint
                    (5, 20, 1.0, 99),   # unknown pickup zone
                    (6, -10, 1.0, 1)],  # dropoff before pickup
                   "double")
    _, meta = execute_pipeline("Trip Data", update, trip_data_process, target,
                               pk_col="surrogate_key", partition_cols=["year", "month"])

    assert (meta["inserted_records"], meta["quarantined_records"]) == (1, 3)
    assert "type_changed(passenger_count)" in meta["schema_changes"]
    assert dict(spark.read.format("delta").load(target).dtypes)["passenger_count"] == "bigint"
    quarantine = spark.read.format("delta").load(validation.quarantine_path("Trip Data"))
    reasons = sorted(reason for row in quarantine.collect() for reason in row._rejection_reasons)
    assert reasons == ["dropoff_after_pickup", "reference(pulocationid->zones.locationid)", "well_formed"]
