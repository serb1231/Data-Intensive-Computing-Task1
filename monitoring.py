"""Week 3, Task 3: platform monitoring.

execute_pipeline wraps every execution in track(), which appends
  - one row per execution to output_data/monitoring/pipeline_runs, and
  - one row per validation check (record rules and schema checks) to
    output_data/monitoring/validation_results.
Failed and rejected executions are recorded too. No pipeline code calls the monitor
explicitly, so a new dataset is monitored as soon as it goes through execute_pipeline.

`python monitoring.py` answers the operational questions with the Spark SQL queries in
MONITORING_QUERIES (`--query NAME` runs one of them).
"""
import argparse
import hashlib
import os
import uuid
from contextlib import contextmanager
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField, StructType,
                               TimestampType)

MONITORING_DIR = "output_data/monitoring"
PIPELINE_RUNS = "pipeline_runs"
VALIDATION_RESULTS = "validation_results"

PIPELINE_RUNS_SCHEMA = StructType([
    StructField("run_id", StringType(), False),          # one invocation of a pipeline script
    StructField("execution_id", StringType(), False),    # one dataset within that run
    StructField("pipeline", StringType(), True),         # initial_load | incremental_update | adhoc
    StructField("dataset", StringType(), False),
    StructField("source", StringType(), True),
    StructField("target_path", StringType(), True),
    StructField("status", StringType(), False),          # succeeded | rejected | failed
    StructField("error_message", StringType(), True),
    StructField("started_at", TimestampType(), False),
    StructField("finished_at", TimestampType(), False),
    StructField("execution_time_seconds", DoubleType(), True),
    StructField("validate_seconds", DoubleType(), True),
    StructField("write_seconds", DoubleType(), True),
    StructField("schema_version", IntegerType(), True),
    StructField("schema_fingerprint", StringType(), True),
    StructField("schema_changes", StringType(), True),
    StructField("write_mode", StringType(), True),
    StructField("source_records", LongType(), True),
    StructField("out_of_scope_records", LongType(), True),
    StructField("processed_records", LongType(), True),
    StructField("rejected_records", LongType(), True),
    StructField("quarantined_records", LongType(), True),
    StructField("dropped_records", LongType(), True),
    StructField("warning_records", LongType(), True),
    StructField("final_clean_records", LongType(), True),
    StructField("inserted_records", LongType(), True),
    StructField("duplicates_skipped", LongType(), True),
    StructField("validation_failures", IntegerType(), True),
    StructField("target_version", LongType(), True),
])

VALIDATION_RESULTS_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("execution_id", StringType(), False),
    StructField("dataset", StringType(), False),
    StructField("recorded_at", TimestampType(), False),
    StructField("rule_name", StringType(), False),
    StructField("category", StringType(), True),
    StructField("action", StringType(), True),
    StructField("status", StringType(), False),          # passed | failed | warning | info | skipped
    StructField("failed_records", LongType(), True),
    StructField("checked_records", LongType(), True),
    StructField("details", StringType(), True),
])


def monitoring_enabled() -> bool:
    """PLATFORM_MONITORING=off skips all monitoring writes (to measure their overhead)."""
    return os.environ.get("PLATFORM_MONITORING", "on").lower() not in ("0", "off", "false", "no")


def table_path(name: str) -> str:
    return f"{MONITORING_DIR}/{name}"


# --- runs and executions -------------------------------------------------------

_current_run = {}


def start_run(pipeline: str) -> str:
    """Start a run: every execution recorded until the next start_run shares its run_id."""
    _current_run["run_id"] = datetime.now().strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:6]
    _current_run["pipeline"] = pipeline
    return _current_run["run_id"]


def schema_fingerprint(schema: StructType) -> str:
    """Column names and types, order-insensitive: sources are bound by column name, not position."""
    described = ",".join(sorted(f"{f.name}:{f.dataType.simpleString()}" for f in schema.fields))
    return hashlib.md5(described.encode()).hexdigest()


def resolve_schema_version(spark: SparkSession, dataset: str, fingerprint: str) -> int:
    """Version 1 for a dataset's first schema; every schema not seen before gets the next number."""
    path = table_path(PIPELINE_RUNS)
    if not DeltaTable.isDeltaTable(spark, path):
        return 1
    seen = (spark.read.format("delta").load(path)
            .filter(F.col("dataset") == dataset)
            .groupBy("schema_fingerprint").agg(F.min("schema_version").alias("version"))
            .collect())
    for row in seen:
        if row["schema_fingerprint"] == fingerprint:
            return row["version"]
    return max((row["version"] or 0 for row in seen), default=0) + 1


def describe_source(df: DataFrame) -> str:
    try:
        names = sorted({os.path.basename(path) for path in df.inputFiles()})
    except Exception:
        names = []
    if not names:
        return "in-memory DataFrame"
    return ", ".join(names[:3]) + (f" (+{len(names) - 3} more)" if len(names) > 3 else "")


class Execution:
    """What is known about one dataset's execution; filled in by execute_pipeline."""

    def __init__(self, spark: SparkSession, dataset: str, target_path: str, source: str, source_schema: StructType):
        if not _current_run:
            start_run("adhoc")
        self.spark = spark
        self.run_id = _current_run["run_id"]
        self.pipeline = _current_run["pipeline"]
        self.execution_id = uuid.uuid4().hex[:12]
        self.dataset = dataset
        self.target_path = target_path
        self.source = source
        self.schema_fingerprint = schema_fingerprint(source_schema)
        self.schema_version = (resolve_schema_version(spark, dataset, self.schema_fingerprint)
                               if monitoring_enabled() else None)
        self.started_at = datetime.now()
        self.status = "succeeded"
        self.error_message = None
        self.metrics = {}
        self.checks = []

    def record(self, metrics: dict, checks: list) -> None:
        self.metrics = metrics
        self.status = metrics.get("status", self.status)
        self.checks = checks


def _coerce(value, dtype):
    if value is None:
        return None
    if isinstance(dtype, DoubleType):
        return float(value)
    if isinstance(dtype, (LongType, IntegerType)):
        return int(value)
    if isinstance(dtype, StringType):
        return str(value)
    return value


def _append(spark: SparkSession, name: str, schema: StructType, rows: list) -> None:
    values = [tuple(_coerce(row.get(f.name), f.dataType) for f in schema.fields) for row in rows]
    (spark.createDataFrame(values, schema).coalesce(1)
     .write.format("delta").mode("append").option("mergeSchema", "true").save(table_path(name)))


def write_execution(execution: Execution) -> None:
    if not monitoring_enabled():
        return
    finished_at = datetime.now()
    run = {
        **execution.metrics,
        "run_id": execution.run_id,
        "execution_id": execution.execution_id,
        "pipeline": execution.pipeline,
        "dataset": execution.dataset,
        "source": execution.source,
        "target_path": execution.target_path,
        "status": execution.status,
        "error_message": execution.error_message,
        "started_at": execution.started_at,
        "finished_at": finished_at,
        "schema_version": execution.schema_version,
        "schema_fingerprint": execution.schema_fingerprint,
    }
    if run.get("execution_time_seconds") is None:
        run["execution_time_seconds"] = round((finished_at - execution.started_at).total_seconds(), 2)
    _append(execution.spark, PIPELINE_RUNS, PIPELINE_RUNS_SCHEMA, [run])

    if execution.checks:
        common = {"run_id": execution.run_id, "execution_id": execution.execution_id,
                  "dataset": execution.dataset, "recorded_at": finished_at}
        _append(execution.spark, VALIDATION_RESULTS, VALIDATION_RESULTS_SCHEMA,
                [{**common, **check.as_row()} for check in execution.checks])


@contextmanager
def track(spark: SparkSession, dataset: str, target_path: str, source_df: DataFrame, source_schema: StructType):
    """Record the execution inside the with-block, whether it succeeds, is rejected or raises."""
    execution = Execution(spark, dataset, target_path, describe_source(source_df), source_schema)
    try:
        yield execution
    except Exception as error:
        execution.status = "failed"
        execution.error_message = f"{type(error).__name__}: {error}"[:2000]
        write_execution(execution)
        raise
    write_execution(execution)


# --- operational queries ----------------------------------------------------------

MONITORING_QUERIES = {
    "failing_datasets": (
        "Which dataset fails validation most frequently?",
        """
        SELECT dataset,
               COUNT(*) AS executions,
               SUM(CASE WHEN validation_failures > 0 THEN 1 ELSE 0 END) AS executions_with_failures,
               SUM(validation_failures) AS failed_checks,
               SUM(rejected_records) AS rejected_records,
               ROUND(100.0 * SUM(rejected_records) / NULLIF(SUM(processed_records), 0), 3) AS pct_records_rejected
        FROM pipeline_runs
        GROUP BY dataset
        ORDER BY executions_with_failures DESC, failed_checks DESC, pct_records_rejected DESC
        """),
    "failing_rules": (
        "Which rules fail most often (drill-down of the question above)?",
        """
        SELECT dataset, rule_name, category, action,
               COUNT(*) AS executions_failed,
               SUM(COALESCE(failed_records, 0)) AS failed_records
        FROM validation_results
        WHERE status IN ('failed', 'warning')
        GROUP BY dataset, rule_name, category, action
        ORDER BY executions_failed DESC, failed_records DESC
        LIMIT 25
        """),
    "slowest_datasets": (
        "Which dataset requires the longest processing time?",
        """
        SELECT dataset,
               COUNT(*) AS executions,
               ROUND(AVG(execution_time_seconds), 2) AS avg_seconds,
               ROUND(MAX(execution_time_seconds), 2) AS max_seconds,
               ROUND(SUM(execution_time_seconds), 2) AS total_seconds,
               ROUND(AVG(validate_seconds), 2) AS avg_validate_seconds,
               ROUND(AVG(write_seconds), 2) AS avg_write_seconds,
               ROUND(SUM(processed_records) / SUM(execution_time_seconds)) AS records_per_second
        FROM pipeline_runs
        WHERE status = 'succeeded'
        GROUP BY dataset
        ORDER BY avg_seconds DESC
        """),
    "rejections_per_execution": (
        "How many records were rejected during each execution?",
        """
        SELECT started_at, pipeline, dataset, status, processed_records,
               rejected_records, quarantined_records, dropped_records, warning_records,
               ROUND(100.0 * rejected_records / NULLIF(processed_records, 0), 3) AS pct_rejected,
               inserted_records
        FROM pipeline_runs
        ORDER BY started_at
        """),
    "processing_time_trend": (
        "How has processing time changed over multiple executions?",
        """
        SELECT dataset, started_at, pipeline, processed_records, execution_time_seconds,
               ROUND(execution_time_seconds
                     - LAG(execution_time_seconds) OVER (PARTITION BY dataset ORDER BY started_at), 2)
                   AS change_vs_previous,
               ROUND(AVG(execution_time_seconds) OVER (PARTITION BY dataset ORDER BY started_at
                                                       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2)
                   AS moving_avg_3,
               -- batches differ in size, so throughput is the fair way to compare executions
               ROUND(processed_records / NULLIF(execution_time_seconds, 0)) AS records_per_second
        FROM pipeline_runs
        WHERE status = 'succeeded'
        ORDER BY dataset, started_at
        """),
    "schema_history": (
        "When did each dataset's schema change, and how?",
        """
        SELECT dataset, schema_version,
               MIN(started_at) AS first_seen, MAX(started_at) AS last_seen, COUNT(*) AS executions,
               MIN_BY(schema_changes, started_at) AS changes_when_introduced
        FROM pipeline_runs
        GROUP BY dataset, schema_version
        ORDER BY dataset, schema_version
        """),
    "latest_status": (
        "Current health: the latest execution of every dataset",
        """
        SELECT dataset, started_at, pipeline, status, schema_version, processed_records,
               inserted_records, rejected_records, validation_failures, execution_time_seconds,
               target_version, error_message
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dataset ORDER BY started_at DESC) AS rn
              FROM pipeline_runs)
        WHERE rn = 1
        ORDER BY dataset
        """),
}


def register_views(spark: SparkSession) -> bool:
    """Expose the monitoring tables as the pipeline_runs / validation_results views."""
    found = False
    for name in (PIPELINE_RUNS, VALIDATION_RESULTS):
        if DeltaTable.isDeltaTable(spark, table_path(name)):
            spark.read.format("delta").load(table_path(name)).createOrReplaceTempView(name)
            found = True
    return found


def main() -> None:
    parser = argparse.ArgumentParser(description="Operational report from the monitoring tables (Week 3, Task 3).")
    parser.add_argument("--query", choices=list(MONITORING_QUERIES), help="run a single query (default: all)")
    args = parser.parse_args()

    from data_ingestion import build_spark
    spark = build_spark()
    if not register_views(spark):
        print("no monitoring data yet: run data_ingestion.py first")
        return

    names = [args.query] if args.query else list(MONITORING_QUERIES)
    for name in names:
        question, sql = MONITORING_QUERIES[name]
        print(f"\n=== {name}: {question} ===")
        try:
            spark.sql(sql).show(100, truncate=False)
        except AnalysisException as error:
            print(f"skipped: {str(error).splitlines()[0]}")
    spark.stop()


if __name__ == "__main__":
    main()
