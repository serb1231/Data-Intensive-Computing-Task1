"""Week 3, Task 4: the extended validation framework.

A batch is checked at two levels before anything is written:

1. Dataset level (check_source_schema, check_target_schema): does this release still have
   the shape the platform expects? Changes the platform can absorb -- a declared new column,
   a numeric type change, a missing optional column -- are applied automatically. Anything
   else (a missing required column, a type that cannot be cast) rejects the whole batch.
2. Record level (validate): every Rule is evaluated for every row in a single cached pass.
   A row that breaks a rule is never silently dropped. Depending on the rule's action it is
   quarantined (stored with its reasons), dropped with a count (an exact re-delivery of a
   record that is already loaded), or kept with a warning.

Rules are plain objects listed per dataset in validation_rules.py. The engine in this file
does not change when a rule is added.

`python validation.py` prints the validation report: the latest outcome of every rule per
dataset, and what is in the quarantine tables.
"""
import argparse
import os
import re
from dataclasses import asdict, dataclass, field
from functools import reduce

from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (ByteType, DataType, DateType, IntegerType, LongType, NullType,
                               NumericType, ShortType, StringType, StructType, TimestampNTZType,
                               TimestampType)

QUARANTINE_DIR = "output_data/quarantine"

# what happens to a record that breaks a rule
QUARANTINE = "quarantine"  # excluded from the table, stored with its reasons in the quarantine table
DROP = "drop"              # excluded and counted, not stored (a re-delivery of a record already loaded)
WARN = "warn"              # kept in the table, counted and reported

# the five kinds of problem the framework detects
DUPLICATE = "duplicate"
INVALID_VALUE = "invalid_value"
MISSING_REFERENCE = "missing_reference"
INCOMPLETE = "incomplete"
SCHEMA = "schema"

# what happens to a batch whose schema changed
EVOLVE = "evolve"              # new column: added to the Delta table
FILL_NULL = "fill_null"        # column the table has but this batch lacks: filled with NULL
CAST = "cast"                  # type changed: cast back to the table's type, lossy values flagged
DROP_COLUMN = "drop_column"    # column not declared in the contract: removed before loading
REJECT_BATCH = "reject_batch"  # the batch cannot be loaded safely: nothing is written

# technical columns the framework adds while a batch is in flight; never written to curated tables
MALFORMED_COL = "_malformed_values"
REASON_COLS = {QUARANTINE: "_quarantine_reasons", DROP: "_drop_reasons", WARN: "_warnings"}
FRAMEWORK_COLS = [MALFORMED_COL, *REASON_COLS.values()]

INTEGRAL_TYPES = (ByteType, ShortType, IntegerType, LongType)
TEMPORAL_TYPES = (DateType, TimestampType, TimestampNTZType)


def validation_enabled() -> bool:
    """PLATFORM_VALIDATION=off runs the pipeline without record-level rules (to measure their overhead)."""
    return os.environ.get("PLATFORM_VALIDATION", "on").lower() not in ("0", "off", "false", "no")


def normalize_column_name(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def quoted(name: str) -> Column:
    """Column reference that survives spaces and dots in raw CSV headers."""
    return F.col("`" + name.replace("`", "``") + "`")


def data_schema(df: DataFrame) -> StructType:
    """The batch's own columns, without the framework's technical ones."""
    return StructType([f for f in df.schema.fields if f.name not in FRAMEWORK_COLS])


def is_delta_table(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


# --- typing: read values without losing them silently ------------------------

def conform_types(df: DataFrame, types: dict) -> DataFrame:
    """Cast columns to the given types and record every value that does not survive the cast.

    A plain cast turns 'abc' into NULL and '12.5' into 12 without a trace. Here a value counts
    as malformed when it was present before the cast and is NULL after it or, for integer
    columns, when the cast changed it ('12.5' -> 12). '3.0' -> 3 is lossless and accepted. That
    matters because pandas writes an integer column that contains a single NaN as floats.
    Malformed values are listed as 'column=raw value' in MALFORMED_COL, which the generic
    well_formed rule then turns into a quarantine reason.
    """
    projections, problems = [], []
    for column in df.schema.fields:
        if column.name == MALFORMED_COL:
            continue
        target = types.get(column.name)
        if target is None or column.dataType == target:
            projections.append(quoted(column.name))
            continue
        raw = quoted(column.name)
        typed = raw.cast(target)
        lost = raw.isNotNull() & typed.isNull()
        if isinstance(target, INTEGRAL_TYPES):
            lost = lost | (raw.isNotNull() & (raw.cast("double") != typed.cast("double")))
        projections.append(typed.alias(column.name))
        problems.append(F.when(lost, F.concat(F.lit(f"{column.name}="), raw.cast("string"))))

    if not problems:
        return df
    previous = [quoted(MALFORMED_COL)] if MALFORMED_COL in df.columns else []
    listed = F.concat_ws("; ", *previous, *problems)
    return df.select(*projections, F.when(listed != "", listed).alias(MALFORMED_COL))


# --- rules --------------------------------------------------------------------

@dataclass
class ValidationContext:
    spark: SparkSession
    dataset: str
    target_path: str
    target_exists: bool


class Rule:
    """One validation rule. A rule only says which rows are wrong; its action says what that costs.

    Subclasses implement violated(), a boolean Column that is TRUE for a row breaking the rule.
    NULL counts as passing, so a range rule does not also report missing values (that is what
    NotNull is for). A rule that needs more than the row itself, such as a window over the batch
    or a join against another table, sets batch_level = True and overrides prepare() to add the
    helper column that violated() reads.
    """
    category = INVALID_VALUE
    batch_level = False

    def __init__(self, name: str, category: str = None, action: str = QUARANTINE):
        self.name = name
        self.category = category or type(self).category
        self.action = action

    @property
    def helper(self) -> str:
        """A column name private to this rule, for rules that need prepare()."""
        return "_v_" + re.sub(r"\W+", "_", self.name).strip("_")

    def skip_reason(self, df: DataFrame, ctx: ValidationContext):
        """Why this rule cannot run on this batch, or None if it can.

        The default resolves violated() against the batch. A rule that reads a column this
        release does not have (e.g. humidity before it was introduced) is skipped and
        reported as such, instead of failing the pipeline.
        """
        try:
            df.select(self.violated())
        except AnalysisException as error:
            missing = re.search(r"`([^`]+)` cannot be resolved", str(error))
            return f"column {missing.group(1)} not in batch" if missing else str(error).splitlines()[0][:200]
        return None

    def prepare(self, df: DataFrame, ctx: ValidationContext) -> DataFrame:
        return df

    def violated(self) -> Column:
        raise NotImplementedError

    def missing_columns(self, df: DataFrame, columns) -> str:
        missing = [c for c in columns if c not in df.columns]
        return f"column {', '.join(missing)} not in batch" if missing else None


class NotNull(Rule):
    """Incomplete record: a mandatory attribute is missing."""
    category = INCOMPLETE

    def __init__(self, column: str, name: str = None, category: str = None, action: str = QUARANTINE):
        super().__init__(name or f"not_null({column})", category, action)
        self.column = column

    def violated(self) -> Column:
        return quoted(self.column).isNull()


def not_null(*columns: str, **kwargs) -> list:
    """One NotNull rule per column, so the report says exactly which attribute was missing."""
    return [NotNull(column, **kwargs) for column in columns]


class InRange(Rule):
    """Invalid value: outside [min, max] (either bound optional, both inclusive)."""

    def __init__(self, column: str, min=None, max=None, name: str = None, action: str = QUARANTINE):
        bounds = ",".join("" if b is None else f"{b:g}" for b in (min, max))
        super().__init__(name or f"in_range({column},{bounds})", INVALID_VALUE, action)
        self.column, self.min, self.max = column, min, max

    def violated(self) -> Column:
        value, broken = quoted(self.column), F.lit(False)
        if self.min is not None:
            broken = broken | (value < self.min)
        if self.max is not None:
            broken = broken | (value > self.max)
        return broken


class Positive(Rule):
    """Invalid value: must be strictly greater than zero."""

    def __init__(self, column: str, name: str = None, action: str = QUARANTINE):
        super().__init__(name or f"positive({column})", INVALID_VALUE, action)
        self.column = column

    def violated(self) -> Column:
        return quoted(self.column) <= 0


class AllowedValues(Rule):
    """Invalid value: not one of the codes the data dictionary defines."""

    def __init__(self, column: str, values: list, name: str = None, action: str = QUARANTINE):
        super().__init__(name or f"allowed_values({column})", INVALID_VALUE, action)
        self.column, self.values = column, list(values)

    def violated(self) -> Column:
        return ~quoted(self.column).isin(self.values)


class Check(Rule):
    """Any Spark SQL condition that every valid row satisfies, e.g. 'dropoff >= pickup'."""

    def __init__(self, name: str, condition: str, category: str = INVALID_VALUE, action: str = QUARANTINE):
        super().__init__(name, category, action)
        self.condition = condition

    def violated(self) -> Column:
        return ~F.expr(self.condition)


class WellFormed(Rule):
    """Generic: a value could not be read as its declared type (see conform_types)."""

    def __init__(self):
        super().__init__("well_formed", INVALID_VALUE, QUARANTINE)

    def violated(self) -> Column:
        return F.col(MALFORMED_COL).isNotNull()


class Unique(Rule):
    """Duplicate within the batch: the key occurs more than once.

    The first copy is kept and every later copy is quarantined. "First" prefers a copy that
    passed the row-level rules and breaks ties on a hash of the row's content, so the choice is
    deterministic and never depends on how Spark happened to partition the file.
    """
    category = DUPLICATE
    batch_level = True

    def __init__(self, *columns: str, name: str = None, action: str = QUARANTINE):
        super().__init__(name or f"unique({','.join(columns)})", DUPLICATE, action)
        self.columns = list(columns)

    def skip_reason(self, df, ctx):
        return self.missing_columns(df, self.columns)

    def prepare(self, df, ctx):
        content = [quoted(c) for c in df.columns if c not in FRAMEWORK_COLS and not c.startswith("_v_")]
        window = (Window.partitionBy(*[quoted(c) for c in self.columns])
                  .orderBy(F.size(REASON_COLS[QUARANTINE]), F.xxhash64(*content)))
        return df.withColumn(self.helper, F.row_number().over(window))

    def violated(self) -> Column:
        return F.col(self.helper) > 1


class NotAlreadyLoaded(Rule):
    """Duplicate of a stored record: the key is already in the target table.

    This is a re-delivery of a record that was loaded before. By default it is dropped and
    counted but not stored, so re-running a load is idempotent and does not refill the
    quarantine with millions of copies.
    """
    category = DUPLICATE
    batch_level = True

    def __init__(self, *columns: str, name: str = None, action: str = DROP):
        super().__init__(name or f"not_already_loaded({','.join(columns)})", DUPLICATE, action)
        self.columns = list(columns)

    def skip_reason(self, df, ctx):
        if not ctx.target_exists:
            return "target table does not exist yet (first load)"
        return self.missing_columns(df, self.columns)

    def prepare(self, df, ctx):
        keys = [f"{self.helper}_k{i}" for i in range(len(self.columns))]
        loaded = (ctx.spark.read.format("delta").load(ctx.target_path)
                  .select(*[quoted(c).alias(k) for c, k in zip(self.columns, keys)])
                  .dropDuplicates()
                  .withColumn(self.helper, F.lit(True)))
        matches = reduce(lambda a, b: a & b, [quoted(c) == F.col(k) for c, k in zip(self.columns, keys)])
        return df.join(loaded, matches, "left").drop(*keys)

    def violated(self) -> Column:
        return F.col(self.helper).isNotNull()


class ReferenceExists(Rule):
    """Missing reference: a non-null foreign key with no matching row in a reference table."""
    category = MISSING_REFERENCE
    batch_level = True

    def __init__(self, column: str, ref_path: str, ref_column: str, name: str = None,
                 action: str = QUARANTINE, broadcast: bool = True):
        table = os.path.basename(ref_path.rstrip("/"))
        super().__init__(name or f"reference({column}->{table}.{ref_column})", MISSING_REFERENCE, action)
        self.column, self.ref_path, self.ref_column, self.broadcast = column, ref_path, ref_column, broadcast

    def skip_reason(self, df, ctx):
        if not is_delta_table(ctx.spark, self.ref_path):
            return f"reference table {self.ref_path} does not exist"
        return self.missing_columns(df, [self.column])

    def prepare(self, df, ctx):
        key = f"{self.helper}_key"
        reference = (ctx.spark.read.format("delta").load(self.ref_path)
                     .select(quoted(self.ref_column).alias(key))
                     .dropDuplicates()
                     .withColumn(self.helper, F.lit(True)))
        if self.broadcast:
            reference = F.broadcast(reference)
        return df.join(reference, quoted(self.column) == F.col(key), "left").drop(key)

    def violated(self) -> Column:
        return quoted(self.column).isNotNull() & F.col(self.helper).isNull()


# --- dataset contracts ---------------------------------------------------------

@dataclass
class DatasetContract:
    """What the platform expects of one dataset.

    schema: the declared source columns, including announced additions such as humidity.
        A source column that is not declared here is an unexpected schema change.
    required_columns: source columns without which the batch cannot be processed.
    rules: the dataset-specific record-level rules.
    The generic rules (well_formed, not_null/unique on the primary key, not_already_loaded) are
    added by all_rules() and never need to be listed.
    """
    schema: StructType = None
    required_columns: list = field(default_factory=list)
    rules: list = field(default_factory=list)
    on_unexpected_column: str = DROP_COLUMN
    check_primary_key: bool = True
    skip_already_loaded: bool = True

    def all_rules(self, pk_col: str, df: DataFrame) -> list:
        generic = []
        if MALFORMED_COL in df.columns:
            generic.append(WellFormed())
        if pk_col and self.check_primary_key:
            generic += [NotNull(pk_col), Unique(pk_col)]
        if pk_col and self.skip_already_loaded:
            generic.append(NotAlreadyLoaded(pk_col))
        return generic + list(self.rules)


# --- dataset level: schema changes -----------------------------------------------

@dataclass
class SchemaChange:
    kind: str
    column: str
    details: str
    action: str

    @property
    def blocking(self) -> bool:
        return self.action == REJECT_BATCH

    def __str__(self):
        return f"{self.kind}({self.column}): {self.details} -> {self.action}"


def can_cast(source: DataType, target: DataType) -> bool:
    """Type changes the platform absorbs automatically; everything else needs a person."""
    if isinstance(source, NullType) or isinstance(target, StringType):
        return True
    if isinstance(source, NumericType) and isinstance(target, NumericType):
        return True  # lossy values are still caught row by row (conform_types)
    return isinstance(source, TEMPORAL_TYPES) and isinstance(target, TEMPORAL_TYPES)


def check_source_schema(contract: DatasetContract, source: StructType) -> list:
    """Compare what the producer delivered with the dataset's declared contract."""
    changes = []
    present = {f.name for f in source.fields}
    for column in contract.required_columns:
        if column not in present:
            changes.append(SchemaChange("missing_required_column", column,
                                        "required by the contract but absent from the source", REJECT_BATCH))
    if contract.schema is not None:
        declared = {normalize_column_name(f.name) for f in contract.schema.fields}
        for column in source.fields:
            if column.name not in declared:
                changes.append(SchemaChange("unexpected_column", column.name,
                                            f"{column.dataType.simpleString()} column not declared in the contract",
                                            contract.on_unexpected_column))
    return changes


def check_target_schema(batch: StructType, target: StructType) -> list:
    """Compare the processed batch with the Delta table it is about to be merged into."""
    changes = []
    target_types = {f.name: f.dataType for f in target.fields}
    batch_types = {f.name: f.dataType for f in batch.fields}
    for name, new in batch_types.items():
        old = target_types.get(name)
        if old is None:
            changes.append(SchemaChange("column_added", name, f"new {new.simpleString()} column", EVOLVE))
        elif new != old:
            if can_cast(new, old):
                changes.append(SchemaChange("type_changed", name,
                                            f"{old.simpleString()} -> {new.simpleString()}, cast back to "
                                            f"{old.simpleString()}", CAST))
            else:
                changes.append(SchemaChange("incompatible_type", name,
                                            f"{old.simpleString()} -> {new.simpleString()} cannot be cast safely",
                                            REJECT_BATCH))
    for name, old in target_types.items():
        if name not in batch_types:
            changes.append(SchemaChange("column_missing", name,
                                        f"{old.simpleString()} column absent from this batch, filled with NULL",
                                        FILL_NULL))
    return changes


def align_to_target(df: DataFrame, target: StructType, changes: list) -> DataFrame:
    """Make the batch mergeable: cast changed types back and add the table's missing columns as NULL."""
    df = conform_types(df, {c.column: target[c.column].dataType for c in changes if c.action == CAST})
    for change in changes:
        if change.action == FILL_NULL:
            df = df.withColumn(change.column, F.lit(None).cast(target[change.column].dataType))
    return df


# --- record level: the engine -------------------------------------------------------

@dataclass
class CheckResult:
    """One row of the monitoring validation_results table."""
    rule_name: str
    category: str
    action: str
    status: str  # passed | failed | warning | info | skipped
    failed_records: int = None
    checked_records: int = None
    details: str = None

    def as_row(self) -> dict:
        return asdict(self)


def schema_change_results(changes: list) -> list:
    status = {REJECT_BATCH: "failed", DROP_COLUMN: "warning"}
    return [CheckResult(f"schema:{c.kind}({c.column})", SCHEMA, c.action, status.get(c.action, "info"),
                        details=c.details)
            for c in changes]


@dataclass
class ValidationOutcome:
    flagged: DataFrame  # the batch plus reason columns, materialized (local checkpoint)
    total: int
    quarantined: int
    dropped: int
    warned: int
    results: list

    @property
    def valid_count(self) -> int:
        return self.total - self.quarantined - self.dropped

    def valid(self) -> DataFrame:
        """The rows that go into the table, without any framework columns."""
        keep = (F.size(REASON_COLS[QUARANTINE]) == 0) & (F.size(REASON_COLS[DROP]) == 0)
        return self.flagged.filter(keep).drop(*[c for c in FRAMEWORK_COLS if c in self.flagged.columns])

    def quarantined_records(self) -> DataFrame:
        """The rows to isolate, each with every reason it was rejected for."""
        return (self.flagged.filter(F.size(REASON_COLS[QUARANTINE]) > 0)
                .withColumn("_rejection_reasons", F.concat(F.col(REASON_COLS[QUARANTINE]), F.col(REASON_COLS[DROP])))
                .drop(REASON_COLS[QUARANTINE], REASON_COLS[DROP]))


def _reasons(rules: list) -> Column:
    if not rules:
        return F.array().cast("array<string>")
    return F.array_compact(F.array(*[F.when(rule.violated(), F.lit(rule.name)) for rule in rules]))


def _count_if(condition: Column) -> Column:
    return F.sum(F.when(condition, 1).otherwise(0))


def validate(df: DataFrame, rules: list, ctx: ValidationContext) -> ValidationOutcome:
    """Evaluate every rule on every row; returns the materialized, flagged batch plus per-rule counts.

    Each row gets one array of rule names per action (quarantine / drop / warn). All rules are
    evaluated together, so one pass over the batch answers every rule and a row that breaks
    three rules is reported with all three reasons, not just the first.
    """
    names = [rule.name for rule in rules]
    duplicated = {name for name in names if names.count(name) > 1}
    if duplicated:
        raise ValueError(f"rule names must be unique within a dataset: {sorted(duplicated)}")

    results, row_rules, batch_rules = {}, [], []
    for rule in rules:
        reason = rule.skip_reason(df, ctx)
        if reason:
            results[rule.name] = CheckResult(rule.name, rule.category, rule.action, "skipped", details=reason)
        else:
            (batch_rules if rule.batch_level else row_rules).append(rule)

    # stage 1: rules that look at one row at a time
    for action, column in REASON_COLS.items():
        df = df.withColumn(column, _reasons([r for r in row_rules if r.action == action]))

    # stage 2: rules that need a window or a join. They run after stage 1 so that, for example,
    # Unique can keep the copy of a duplicate that passed the row-level rules.
    for rule in batch_rules:
        df = rule.prepare(df, ctx)
    for action, column in REASON_COLS.items():
        extra = [r for r in batch_rules if r.action == action]
        if extra:
            df = df.withColumn(column, F.concat(F.col(column), _reasons(extra)))

    # materialize once and cut the lineage. A plain persist() is not enough: not_already_loaded
    # reads the target table, so when the batch is merged into it, Delta invalidates the cache
    # and the next use would re-validate against the new table version, where every row is
    # already loaded. The batch handed downstream (e.g. to the integrated table) must stay fixed.
    flagged = df.drop(*[r.helper for r in batch_rules]).localCheckpoint(eager=True)

    evaluated = row_rules + batch_rules
    quarantined = F.size(REASON_COLS[QUARANTINE]) > 0
    dropped = ~quarantined & (F.size(REASON_COLS[DROP]) > 0)
    warned = ~quarantined & ~dropped & (F.size(REASON_COLS[WARN]) > 0)
    stats = flagged.agg(
        F.count(F.lit(1)).alias("total"),
        _count_if(quarantined).alias("quarantined"),
        _count_if(dropped).alias("dropped"),
        _count_if(warned).alias("warned"),
        *[_count_if(F.array_contains(REASON_COLS[rule.action], rule.name)).alias(f"rule_{i}")
          for i, rule in enumerate(evaluated)],
    ).first()

    total = stats["total"] or 0
    for i, rule in enumerate(evaluated):
        failed = stats[f"rule_{i}"] or 0
        status = "passed" if failed == 0 else ("warning" if rule.action == WARN else "failed")
        results[rule.name] = CheckResult(rule.name, rule.category, rule.action, status, failed, total)

    return ValidationOutcome(flagged, total, stats["quarantined"] or 0, stats["dropped"] or 0,
                             stats["warned"] or 0, [results[name] for name in names])


# --- quarantine ---------------------------------------------------------------------------

def quarantine_path(dataset: str) -> str:
    return f"{QUARANTINE_DIR}/{re.sub(r'[^0-9a-z]+', '_', dataset.lower()).strip('_')}"


ROWS_PER_FILE = 1_000_000


def files_for(rows: int) -> int:
    """How many files to write `rows` rows into: about a million rows per file, at least one."""
    return max(1, -(-rows // ROWS_PER_FILE))


def write_quarantine(records: DataFrame, rows: int, dataset: str, run_id: str, execution_id: str) -> str:
    """Append rejected records to the dataset's quarantine Delta table.

    Records keep their own columns and types, so they can be queried like the curated table,
    fixed and re-submitted. mergeSchema lets the quarantine follow the dataset's schema
    evolution (e.g. gain humidity) without a separate migration.
    """
    path = quarantine_path(dataset)
    (records
     .coalesce(files_for(rows))
     .withColumn("_dataset", F.lit(dataset))
     .withColumn("_run_id", F.lit(run_id))
     .withColumn("_execution_id", F.lit(execution_id))
     .withColumn("_quarantined_at", F.current_timestamp())
     .write.format("delta").mode("append").option("mergeSchema", "true").save(path))
    return path


# --- report -----------------------------------------------------------------------------------

def show_validation_report(spark: SparkSession, samples: int = 5) -> None:
    from monitoring import register_views
    if not register_views(spark):
        print("no monitoring data yet: run data_ingestion.py first")
        return

    print("\n=== Rule outcomes of the latest execution per dataset (passed rules omitted) ===")
    spark.sql("""
        WITH latest AS (
            SELECT dataset, execution_id,
                   ROW_NUMBER() OVER (PARTITION BY dataset ORDER BY started_at DESC) AS rn
            FROM pipeline_runs
        )
        SELECT v.dataset, v.rule_name, v.category, v.action, v.status,
               v.failed_records, v.checked_records, v.details
        FROM validation_results v JOIN latest l
          ON v.execution_id = l.execution_id AND l.rn = 1
        WHERE v.status <> 'passed'
        ORDER BY v.dataset, v.status, v.failed_records DESC
    """).show(100, truncate=80)

    print("\n=== Validation statistics per category, all executions ===")
    spark.sql("""
        SELECT dataset, category,
               SUM(CASE WHEN status IN ('failed', 'warning') THEN 1 ELSE 0 END) AS checks_with_findings,
               SUM(COALESCE(failed_records, 0)) AS records_flagged
        FROM validation_results
        GROUP BY dataset, category
        HAVING checks_with_findings > 0
        ORDER BY dataset, records_flagged DESC
    """).show(100, truncate=False)

    if not os.path.isdir(QUARANTINE_DIR):
        return
    for name in sorted(os.listdir(QUARANTINE_DIR)):
        path = f"{QUARANTINE_DIR}/{name}"
        if not is_delta_table(spark, path):
            continue
        quarantine = spark.read.format("delta").load(path)
        print(f"\n=== Quarantine {path}: {quarantine.count()} records, by reason ===")
        (quarantine.select(F.explode("_rejection_reasons").alias("reason"))
         .groupBy("reason").count().orderBy(F.desc("count")).show(50, truncate=False))
        detail = ["_rejection_reasons", "_run_id"] + ([MALFORMED_COL] if MALFORMED_COL in quarantine.columns else [])
        data = [c for c in quarantine.columns if not c.startswith("_")][:6]
        quarantine.select(*detail, *data).show(samples, truncate=60)


def main() -> None:
    parser = argparse.ArgumentParser(description="Print the validation report (Week 3, Task 4).")
    parser.add_argument("--samples", type=int, default=5, help="quarantined rows to show per dataset")
    args = parser.parse_args()

    from data_ingestion import build_spark
    spark = build_spark()
    show_validation_report(spark, args.samples)
    spark.stop()


if __name__ == "__main__":
    main()
