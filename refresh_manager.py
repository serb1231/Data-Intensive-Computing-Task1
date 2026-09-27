import argparse
import json
import os
import time
from dataclasses import dataclass, field

import data_products  # sets PYSPARK_SUBMIT_ARGS before pyspark is imported
from data_products import (PARTITION_COLS, PRODUCTS, METADATA_COLS, REGISTRY_PATH, SOURCE_TABLE,
                           discover_window, format_months, product_exists, product_path,
                           register_report_view, release_report_view, source_delta_version)
from analytical_queries import INTEGRATED_PATH, register_views
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

FULL, INCREMENTAL, SKIP, BLOCKED = "full", "incremental", "skip", "blocked"


@dataclass
class ProductPlan:
    name: str
    mode: str
    reason: str
    months: list = field(default_factory=list)  # incremental: the partitions to rebuild


@dataclass
class RefreshPlan:
    source_version: int
    window: list
    products: list

    def of_mode(self, mode: str) -> list:
        return [p for p in self.products if p.mode == mode]


# --- what changed ------------------------------------------------------------------

def changed_partitions(table_path: str, after_version: int, upto_version: int):
    """(year, month) of every file added or removed by a data-changing commit in (after, upto].

    Returns None when a commit file is missing (log retention removed it): then the changes
    cannot be known and the caller has to fall back to a full refresh. Compaction commits
    (OPTIMIZE) mark their files dataChange=false and are ignored: they rewrite, not change.
    """
    months = set()
    for version in range(after_version + 1, upto_version + 1):
        log_file = os.path.join(table_path, "_delta_log", f"{version:020d}.json")
        if not os.path.exists(log_file):
            return None
        with open(log_file) as handle:
            for line in handle:
                action = json.loads(line)
                change = action.get("add") or action.get("remove")
                if not change or not change.get("dataChange", True):
                    continue
                values = change.get("partitionValues") or {}
                year, month = values.get("year"), values.get("month")
                if year is not None and month is not None:
                    months.add((int(year), int(month)))
    return months


def latest_registry_rows(spark: SparkSession) -> dict:
    """The last refresh of every product: the version and schema it was built from."""
    if not os.path.isdir(os.path.join(REGISTRY_PATH, "_delta_log")):
        return {}
    registry = spark.read.format("delta").load(REGISTRY_PATH)
    if "source_columns" not in registry.columns:  # a Week 2 registry: no schema recorded yet
        registry = registry.withColumn("source_columns", F.lit(None).cast("string"))
    rows = (registry
            .withColumn("rn", F.row_number().over(
                Window.partitionBy("product_name").orderBy(F.col("refreshed_at").desc())))
            .filter("rn = 1")
            .select("product_name", "source_version", "schema_version", "source_columns")
            .collect())
    return {row["product_name"]: row for row in rows}


def product_months(spark: SparkSession, name: str) -> set:
    rows = spark.read.format("delta").load(product_path(name)).select(*PARTITION_COLS).distinct().collect()
    return {(row["year"], row["month"]) for row in rows}


# --- the plan ------------------------------------------------------------------------

def plan_product(spark: SparkSession, name: str, last, source_types: dict, source_version: int,
                 window: list, changes_since: dict) -> ProductPlan:
    spec = PRODUCTS[name]

    missing = [c for c in spec["reads"] if c not in source_types]
    if missing:
        return ProductPlan(name, BLOCKED, f"{SOURCE_TABLE} lost required column(s) {missing}: "
                                          f"the product definition has to be adapted by hand")
    if not product_exists(name) or last is None:
        return ProductPlan(name, FULL, "product does not exist yet")
    if last["schema_version"] != spec["version"]:
        return ProductPlan(name, FULL, f"definition changed (version {last['schema_version']} -> {spec['version']})")
    if last["source_columns"] is None:
        return ProductPlan(name, FULL, "no record of the source schema it was built from")

    built_from = json.loads(last["source_columns"])
    retyped = [f"{c} {built_from[c]} -> {source_types[c]}"
               for c in built_from if c in source_types and source_types[c] != built_from[c]]
    if retyped:
        return ProductPlan(name, FULL, f"type of a column it reads changed ({', '.join(retyped)})")

    if last["source_version"] >= source_version:
        return ProductPlan(name, SKIP, f"already built from version {source_version}")

    # the log is read once per distinct starting version, not once per product
    start = last["source_version"]
    if start not in changes_since:
        changes_since[start] = changed_partitions(INTEGRATED_PATH, start, source_version)
    changed = changes_since[start]
    if changed is None:
        return ProductPlan(name, FULL, f"change history since version {start} no longer available")

    stored = product_months(spark, name)
    window_set = set(window)
    # a changed month is rebuilt if the product reports on it (window) or still holds it
    # (a month that dropped out of the window is rebuilt empty, i.e. removed); a month that
    # entered the window without a data change cannot happen, but is covered anyway
    affected = (changed & (window_set | stored)) | (window_set - stored)

    appeared = [c for c in spec.get("optional", []) if c in source_types and c not in built_from]
    note = f"; optional column(s) {appeared} now present" if appeared else ""
    if not affected:
        outside = changed - window_set
        detail = (f"changes only outside the window ({format_months(outside)})" if outside
                  else "no change in any month it reports on")
        return ProductPlan(name, SKIP, detail + note)
    return ProductPlan(name, INCREMENTAL, f"source changed in {format_months(affected)} "
                                          f"(versions {start + 1}..{source_version}){note}",
                       sorted(affected))


def build_plan(spark: SparkSession, names: list = None, force_full: bool = False) -> RefreshPlan:
    source = spark.read.format("delta").load(INTEGRATED_PATH)
    source_types = {f.name: f.dataType.simpleString() for f in source.schema.fields}
    source_version = source_delta_version(spark)
    window = discover_window(spark, source)
    registry = latest_registry_rows(spark)

    plans, changes_since = [], {}
    for name in names or list(PRODUCTS):
        plan = plan_product(spark, name, registry.get(name), source_types, source_version, window, changes_since)
        if force_full and plan.mode != BLOCKED:
            plan = ProductPlan(name, FULL, "full refresh requested")
        plans.append(plan)
    return RefreshPlan(source_version, window, plans)


def print_plan(plan: RefreshPlan) -> None:
    print("\n" + "=" * 78)
    print("REFRESH PLAN")
    print("=" * 78)
    print(f"source            : {INTEGRATED_PATH} @ version {plan.source_version}")
    print(f"reporting window  : {format_months(plan.window)}")
    for p in plan.products:
        target = format_months(p.months) if p.mode == INCREMENTAL else ("all" if p.mode == FULL else "-")
        print(f"  {p.name:<28}{p.mode.upper():<13}{target}")
        print(f"  {'':<28}reason: {p.reason}")
    counts = {mode: len(plan.of_mode(mode)) for mode in (FULL, INCREMENTAL, SKIP, BLOCKED)}
    print("  => " + ", ".join(f"{n} {mode}" for mode, n in counts.items()))


# --- executing it ----------------------------------------------------------------------

def execute_plan(spark: SparkSession, plan: RefreshPlan, verbose: bool = False) -> dict:
    """Run the plan; products that need the same months share one cached slice of the source."""
    start = time.time()
    rows = []
    full = [p.name for p in plan.of_mode(FULL)]
    if full:
        rows += data_products.refresh(spark, full, plan.source_version, plan.window,
                                      reason="; ".join(sorted({p.reason for p in plan.of_mode(FULL)})),
                                      verbose=verbose)
    groups = {}
    for p in plan.of_mode(INCREMENTAL):
        groups.setdefault(tuple(p.months), []).append(p)
    for months, group in groups.items():
        rows += data_products.refresh(spark, [p.name for p in group], plan.source_version, plan.window,
                                      months=list(months), reason=group[0].reason, verbose=verbose)

    summary = {
        "source_version": plan.source_version,
        "window": format_months(plan.window),
        "seconds": round(time.time() - start, 2),
        "products": {p.name: {"mode": p.mode, "reason": p.reason,
                              "months": format_months(p.months) if p.mode == INCREMENTAL else None}
                     for p in plan.products},
        "refreshes": [{k: row[k] for k in ("product_name", "refresh_mode", "refreshed_partitions",
                                           "build_seconds", "row_count", "size_bytes")} for row in rows],
    }
    for row in rows:
        print(f"  {row['product_name']:<28}{row['refresh_mode']:<13}{row['refreshed_partitions']:<28}"
              f"{row['build_seconds']:>6.2f}s {row['row_count']:>8,} rows")
    print(f"  refresh took {summary['seconds']:.2f}s"
          + ("" if rows else " (every product is up to date; no product was rebuilt)"))
    return summary


def refresh_affected(spark: SparkSession, names: list = None, force_full: bool = False,
                     dry_run: bool = False) -> dict:
    """Plan and (unless dry_run) execute the refresh. The entry point for continuous_data_insert.py."""
    register_views(spark)  # re-read the tables: they may have changed since the views were made
    plan = build_plan(spark, names, force_full)
    print_plan(plan)
    if dry_run:
        print("--dry-run: nothing was refreshed")
        return {"plan": plan}
    return execute_plan(spark, plan)


# --- verification: does incremental equal full? -------------------------------------------

def verify(spark: SparkSession, names: list = None, tolerance: float = 0.0015) -> dict:
    """Compare every stored product with the same product built from scratch over the window.

    Rows are matched on their grain (the non-measure columns), and every numeric measure must
    agree within `tolerance`. The products round to 2-3 decimals, and a sum computed in a
    different order can land on the other side of a rounding boundary, so a last-digit
    difference is allowed; anything larger is a real inconsistency. The two compatibility
    views are also compared with their Week 2 definition.
    """
    register_views(spark)
    names = names or list(PRODUCTS)
    window = discover_window(spark)
    register_report_view(spark, window, names)
    results = {}
    try:
        for name in names:
            expected = PRODUCTS[name]["build"](spark)
            stored = spark.read.format("delta").load(product_path(name)).drop(*METADATA_COLS)
            results[name] = compare(expected, stored, tolerance)
        for name, week2_build in WEEK2_DEFINITIONS.items():
            if name in names:
                data_products.register_product_views(spark)
                expected = spark.sql(week2_build)
                view = spark.table(name).drop(*METADATA_COLS)
                results[f"{name} (Week 2 view)"] = compare(expected, view, tolerance)
    finally:
        release_report_view(spark)

    print("\n=== verification: stored product vs. a from-scratch build ===")
    for name, r in results.items():
        status = "OK" if r["consistent"] else "MISMATCH"
        print(f"  {name:<44}{status:<10}rows {r['stored_rows']:>7,} vs {r['expected_rows']:>7,}   "
              f"missing {r['missing_rows']}, extra {r['extra_rows']}, "
              f"values off by > {tolerance}: {r['value_mismatches']}")
    return results


def compare(expected: DataFrame, stored: DataFrame, tolerance: float) -> dict:
    """Match rows on the grain columns; every numeric measure must agree within tolerance."""
    numeric = ("double", "float", "bigint", "int", "decimal", "smallint", "tinyint")
    grain_numbers = set(PARTITION_COLS) | {"day_of_week_num", "hour_of_day"}
    measures = [f.name for f in expected.schema.fields
                if f.dataType.simpleString().startswith(numeric) and f.name not in grain_numbers]
    keys = [c for c in expected.columns if c not in measures]

    e = expected.select(*keys, *[F.col(m).alias(f"e_{m}") for m in measures], F.lit(1).alias("_in_expected"))
    s = stored.select(*[F.col(k).alias(f"s_{k}") for k in keys],
                      *[F.col(m).alias(f"s_{m}") for m in measures], F.lit(1).alias("_in_stored"))
    on = [F.col(k).eqNullSafe(F.col(f"s_{k}")) for k in keys]
    joined = e.join(s, on, "full_outer")

    differs = F.lit(False)
    for m in measures:
        a, b = F.col(f"e_{m}"), F.col(f"s_{m}")
        differs = differs | (a.isNull() != b.isNull()) | F.coalesce(F.abs(a - b) > tolerance, F.lit(False))
    both = F.col("_in_expected").isNotNull() & F.col("_in_stored").isNotNull()

    counts = joined.agg(
        F.sum(F.col("_in_expected")).alias("expected_rows"),
        F.sum(F.col("_in_stored")).alias("stored_rows"),
        F.sum(F.when(F.col("_in_stored").isNull(), 1).otherwise(0)).alias("missing_rows"),
        F.sum(F.when(F.col("_in_expected").isNull(), 1).otherwise(0)).alias("extra_rows"),
        F.sum(F.when(both & differs, 1).otherwise(0)).alias("value_mismatches"),
    ).first().asDict()
    counts = {k: int(v or 0) for k, v in counts.items()}
    counts["consistent"] = counts["missing_rows"] == counts["extra_rows"] == counts["value_mismatches"] == 0
    return counts


# the Week 2 definitions of the two re-grained products, computed straight from the trips:
# what their compatibility views must still return
WEEK2_DEFINITIONS = {
    "weather_impact_summary": """
        SELECT weather_category, pickup_zone, pickup_borough,
               COUNT(*)                        AS trip_count,
               ROUND(SUM(trip_distance), 2)    AS total_distance_miles,
               ROUND(SUM(total_amount), 2)     AS total_revenue,
               ROUND(AVG(trip_distance), 3)    AS avg_trip_distance,
               ROUND(STDDEV(trip_distance), 3) AS stddev_trip_distance
        FROM trips
        WHERE weather_category <> 'Unknown' AND pickup_zone IS NOT NULL
        GROUP BY weather_category, pickup_zone, pickup_borough""",
    "borough_mobility_summary": """
        SELECT pickup_borough,
               date_format(tpep_pickup_datetime, 'EEEE') AS day_of_week,
               dayofweek(tpep_pickup_datetime)           AS day_of_week_num,
               hour(tpep_pickup_datetime)                AS hour_of_day,
               COUNT(*)                                  AS trip_count,
               ROUND(SUM(trip_distance), 2)              AS total_distance_miles,
               ROUND(SUM(total_amount), 2)               AS total_revenue,
               ROUND(AVG(trip_distance), 3)              AS avg_trip_distance
        FROM trips
        WHERE pickup_borough IS NOT NULL
        GROUP BY pickup_borough, day_of_week, day_of_week_num, hour_of_day""",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh only the data products affected by new data (Week 3, Task 2).")
    parser.add_argument("--product", action="append", choices=list(PRODUCTS),
                        help="limit to this product (repeatable; default: all)")
    parser.add_argument("--dry-run", action="store_true", help="print the plan without refreshing")
    parser.add_argument("--full", action="store_true", help="rebuild every product over the whole window")
    parser.add_argument("--verify", action="store_true",
                        help="after refreshing, compare every product with a from-scratch build")
    args = parser.parse_args()

    from data_ingestion import build_spark
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    refresh_affected(spark, args.product, force_full=args.full, dry_run=args.dry_run)
    if args.verify:
        verify(spark, args.product)
    if not args.dry_run:
        data_products.show_registry(spark)
    spark.stop()


if __name__ == "__main__":
    main()
