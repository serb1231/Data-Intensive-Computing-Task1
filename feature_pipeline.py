"""Week 4, Task 2: the reusable feature engineering pipeline.

feature_pipeline() returns an unfitted Spark ML Pipeline that turns rows of the training dataset
(Task 1) into one `features` vector per row:

  1. calendar  SQLTransformer    hour of day, day of week, month, weekend and holiday flags from pickup_hour
  2. prepare   SQLTransformer    log1p of the skewed counts; a 0/1 flag for each quantity missing in train
  3. impute    Imputer           a missing quantity becomes its median in the train split
  4. encode    StringIndexer,    a code becomes one indicator per category; NULL, or a category not seen
               OneHotEncoder     in training (a new zone), becomes all zeros
  5. scale     StandardScaler    quantities to mean 0 and standard deviation 1; codes and flags are not scaled
  6. assemble  VectorAssembler   scaled quantities, indicators and flags -> features

Which column goes through which stage comes from ml_config.py and from a profile of the train
split (plan_features): a feature that is mostly missing or constant in the train split is dropped
and reported. A new feature therefore needs a line in ml_config.py and no change here. All stages
are standard Spark ML stages, so the fitted pipeline saves and loads like any Spark model, and
Task 3 appends a regressor to the same stages. Every estimator is fitted on the train split only.

`python feature_pipeline.py` fits the pipeline, transforms the three splits and writes them to
output_data/ml/<name> (Delta, partitioned by split: keys, target, split and features), saves the
fitted pipeline to output_data/ml/models/<name>, and records the plan, what each stage learned,
the layout of the feature vector and a probe of each source's value in feature_pipeline_week_4.json.
"""
import argparse
import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime

from training_dataset import latest_version, read_version  # sets PYSPARK_SUBMIT_ARGS via data_products
import ml_config
from ml_config import FeaturePipelineConfig, TrainingDatasetConfig
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import (Imputer, ImputerModel, OneHotEncoder, SQLTransformer, StandardScaler,
                                StandardScalerModel, StringIndexer, StringIndexerModel, VectorAssembler)
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FEATURES = "features"
SCALED = "numeric_scaled"


@dataclass
class FeaturePlan:
    """Which input column goes through which stage, decided from ml_config.py and the train split."""
    numeric: list = field(default_factory=list)        # quantities: imputed and scaled
    log: list = field(default_factory=list)            # the quantities that are log1p-transformed first
    missing_flags: list = field(default_factory=list)  # quantities missing somewhere in train: + <column>_missing
    categorical: list = field(default_factory=list)    # codes: one-hot encoded
    binary: list = field(default_factory=list)         # 0/1 flags: used as they are
    dropped: dict = field(default_factory=dict)        # column -> why it is not a feature

    def prepared(self, column: str) -> str:
        return f"{column}_log" if column in self.log else column

    def imputed(self, column: str) -> str:
        return f"{self.prepared(column)}_imputed"


# --- the plan ------------------------------------------------------------------------------

def calendar_stage(config: FeaturePipelineConfig) -> SQLTransformer:
    derived = ", ".join(f"{expression} AS {column}" for column, expression in config.calendar_features.items())
    return SQLTransformer(statement=f"SELECT *, {derived} FROM __THIS__")


def plan_features(train: DataFrame, dataset: TrainingDatasetConfig, config: FeaturePipelineConfig) -> FeaturePlan:
    """Sort the candidate features into the stages, by their declared kind and a profile of the train split.

    A column is a code if ml_config declares it categorical, a flag if it declares it binary, and a
    quantity otherwise. A feature missing in more than max_null_share of the train split, or with a
    single value in it, gives a model nothing to learn from and is dropped instead of breaking the
    imputer or wasting a slot.
    """
    candidates = [*dataset.feature_columns, *config.calendar_features]
    categorical = set(dataset.categorical_features) | set(config.categorical_calendar)
    profiled = calendar_stage(config).transform(train)
    profile = profiled.agg(*[F.avg(F.col(c).isNull().cast("double")).alias(f"null:{c}") for c in candidates],
                           *[F.min(c).alias(f"min:{c}") for c in candidates],
                           *[F.max(c).alias(f"max:{c}") for c in candidates]).first()

    plan = FeaturePlan()
    for column in candidates:
        null_share, low, high = profile[f"null:{column}"], profile[f"min:{column}"], profile[f"max:{column}"]
        if null_share > config.max_null_share:
            plan.dropped[column] = f"missing in {null_share:.0%} of the train split"
        elif low == high:
            plan.dropped[column] = f"constant in the train split ({low})"
        elif column in categorical:
            plan.categorical.append(column)
        elif column in config.binary_features:
            plan.binary.append(column)
        else:
            plan.numeric.append(column)
            if column in config.log_features:
                plan.log.append(column)
            if null_share > 0:
                plan.missing_flags.append(column)
    return plan


# --- the pipeline --------------------------------------------------------------------------

def build_feature_pipeline(plan: FeaturePlan, config: FeaturePipelineConfig) -> Pipeline:
    """The unfitted stages that turn a row of the training dataset into its features vector."""
    stages = [calendar_stage(config)]

    # the flags are taken before imputation, so the model still sees that a value was missing
    prepare = ([f"log1p({c}) AS {plan.prepared(c)}" for c in plan.log]
               + [f"CAST({c} IS NULL AS DOUBLE) AS {c}_missing" for c in plan.missing_flags])
    if prepare:
        stages.append(SQLTransformer(statement=f"SELECT *, {', '.join(prepare)} FROM __THIS__"))

    assembled = []
    if plan.numeric:
        imputed = [plan.imputed(c) for c in plan.numeric]
        stages += [
            Imputer(inputCols=[plan.prepared(c) for c in plan.numeric], outputCols=imputed, strategy="median"),
            VectorAssembler(inputCols=imputed, outputCol="numeric"),
            StandardScaler(inputCol="numeric", outputCol=SCALED, withMean=True, withStd=True),
        ]
        assembled.append(SCALED)
    if plan.categorical:
        # alphabetical order keeps a category's slot stable when frequencies change between retrainings;
        # "keep" sends NULL and unseen categories to an extra index that the encoder drops (all zeros)
        indexes, onehots = [f"{c}_index" for c in plan.categorical], [f"{c}_onehot" for c in plan.categorical]
        stages += [
            StringIndexer(inputCols=plan.categorical, outputCols=indexes, handleInvalid="keep",
                          stringOrderType="alphabetAsc"),
            OneHotEncoder(inputCols=indexes, outputCols=onehots),
        ]
        assembled += onehots
    assembled += plan.binary + [f"{c}_missing" for c in plan.missing_flags]
    stages.append(VectorAssembler(inputCols=assembled, outputCol=FEATURES))
    return Pipeline(stages=stages)


def feature_pipeline(train: DataFrame, dataset: TrainingDatasetConfig = ml_config.TRAINING_DATASET,
                     config: FeaturePipelineConfig = ml_config.FEATURE_PIPELINE) -> tuple:
    """(pipeline, plan) for this train split. Task 3 appends its regressor to pipeline.getStages()."""
    plan = plan_features(train, dataset, config)
    return build_feature_pipeline(plan, config), plan


# --- what was built --------------------------------------------------------------------------

def feature_layout(transformed: DataFrame, plan: FeaturePlan) -> list:
    """[(slot name, input column)] for every slot of the features vector, in order."""
    attrs = transformed.schema[FEATURES].metadata["ml_attr"]["attrs"]
    slots = sorted((attr["idx"], attr["name"]) for group in attrs.values() for attr in group)
    numeric = {f"{SCALED}_{i}": column for i, column in enumerate(plan.numeric)}
    layout = []
    for _, name in slots:
        if name in numeric:
            layout.append((plan.prepared(numeric[name]), numeric[name]))
        elif "_onehot_" in name:
            column, value = name.split("_onehot_", 1)
            layout.append((f"{column}={value}", column))
        elif name.endswith("_missing") and name[:-len("_missing")] in plan.missing_flags:
            layout.append((name, name[:-len("_missing")]))
        else:
            layout.append((name, name))
    return layout


def preprocessing_steps(plan: FeaturePlan, config: FeaturePipelineConfig) -> dict:
    """input column -> the steps it goes through, in order."""
    steps = {}
    for column in [*plan.numeric, *plan.categorical, *plan.binary]:
        done = ["derived from pickup_hour"] if column in config.calendar_features else []
        if column in plan.numeric:
            done += (["log1p"] if column in plan.log else []) \
                    + (["missing flag"] if column in plan.missing_flags else []) \
                    + ["median imputation", "standard scaling"]
        elif column in plan.categorical:
            done += ["string indexing", "one-hot encoding"]
        steps[column] = done or ["used as it is"]
    return steps


def learned_values(model: PipelineModel, plan: FeaturePlan) -> dict:
    """What the estimators learned from the train split: medians, scaling and category counts."""
    learned = {}
    for stage in model.stages:
        if isinstance(stage, ImputerModel):
            learned["imputed_with"] = {c: round(v, 4) for c, v in stage.surrogateDF.first().asDict().items()}
        elif isinstance(stage, StandardScalerModel):
            learned["scaled_with"] = {plan.prepared(c): {"mean": round(m, 4), "std": round(s, 4)}
                                      for c, m, s in zip(plan.numeric, stage.mean, stage.std)}
        elif isinstance(stage, StringIndexerModel):
            learned["categories"] = {c: len(labels) for c, labels in zip(plan.categorical, stage.labelsArray)}
    return learned


def source_of(column: str, dataset: TrainingDatasetConfig, config: FeaturePipelineConfig) -> str:
    """The dataset of the platform an input column comes from."""
    if column in config.calendar_features:
        return "trips: pickup time"
    if column in dataset.lag_columns:
        return "trips: demand history"
    if column == "pulocationid" or column in dataset.zone_features.values():
        return "taxi_zones"
    for source, columns in dataset.hourly_features.items():
        if column in columns:
            return source
    return "other"


def probe_sources(features: DataFrame, layout: list, dataset: TrainingDatasetConfig,
                  config: FeaturePipelineConfig) -> dict:
    """Which sources the useful features come from, according to a random forest fitted on the train split.

    A probe of the features, not the Task 3 model: the importances are summed per source dataset,
    and the validation score only shows that the features can be learned from.
    """
    target = dataset.target
    forest = RandomForestRegressor(featuresCol=FEATURES, labelCol=target, numTrees=30, maxDepth=10,
                                   subsamplingRate=0.5, seed=42).fit(features.where("split = 'train'"))
    importances = forest.featureImportances.toArray()

    by_source = {}
    for (_, column), importance in zip(layout, importances):
        source = source_of(column, dataset, config)
        by_source[source] = by_source.get(source, 0.0) + float(importance)
    top = sorted(zip((name for name, _ in layout), importances), key=lambda pair: -pair[1])[:12]

    predictions = forest.transform(features.where("split = 'validation'"))
    score = lambda metric: round(RegressionEvaluator(labelCol=target, metricName=metric).evaluate(predictions), 4)
    return {
        "model": "RandomForestRegressor(numTrees=30, maxDepth=10, subsamplingRate=0.5, seed=42)",
        "importance_by_source": {s: round(v, 4) for s, v in sorted(by_source.items(), key=lambda kv: -kv[1])},
        "top_features": {name: round(float(v), 4) for name, v in top},
        "validation": {"r2": score("r2"), "rmse": score("rmse"), "mae": score("mae")},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fit the Week 4 feature pipeline (Task 2) on the train split "
                                                 "and write the feature dataset.")
    parser.add_argument("--no-probe", action="store_true",
                        help="skip the random-forest probe of each source's value (about a minute)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dataset_config, config = ml_config.TRAINING_DATASET, ml_config.FEATURE_PIPELINE
    target = dataset_config.target

    from data_ingestion import build_spark
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    dataset_version = latest_version(spark, dataset_config.path)
    dataset = read_version(spark, dataset_config.path, dataset_version)
    train = dataset.where("split = 'train'").cache()

    started = time.time()
    pipeline, plan = feature_pipeline(train, dataset_config, config)
    model = pipeline.fit(train)
    fit_seconds = round(time.time() - started, 2)
    print(f"plan: {len(plan.numeric)} quantities, {len(plan.categorical)} codes, {len(plan.binary)} flags; "
          f"dropped {plan.dropped or 'nothing'}")

    model.write().overwrite().save(config.model_path)

    # what the model needs and nothing else: the keys to trace a row, the target, the vector, the split
    started = time.time()
    features = model.transform(dataset).select("pickup_hour", "pulocationid", target, FEATURES, "split")
    (features.repartition("split")
     .write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true")
     .option("userMetadata", json.dumps({"feature_pipeline": config.name, "model_path": config.model_path,
                                         "source": dataset_config.path, "source_version": dataset_version,
                                         "plan": asdict(plan)}))
     .partitionBy("split")
     .save(config.path))
    write_seconds = round(time.time() - started, 2)

    written = spark.read.format("delta").load(config.path)
    layout = feature_layout(written, plan)
    rows = {r["split"]: r["count"] for r in written.groupBy("split").count().collect()}

    probe, probe_seconds = None, None
    if not args.no_probe:
        started = time.time()
        probe = probe_sources(written.cache(), layout, dataset_config, config)
        probe_seconds = round(time.time() - started, 2)

    manifest = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "fit_seconds": fit_seconds,
        "transform_and_write_seconds": write_seconds,
        "probe_seconds": probe_seconds,
        "input": {"path": dataset_config.path, "delta_version": dataset_version},
        "output": {"path": config.path, "delta_version": latest_version(spark, config.path),
                   "model_path": config.model_path, "rows": rows, "feature_count": len(layout)},
        "plan": asdict(plan),
        "preprocessing": preprocessing_steps(plan, config),
        "learned": learned_values(model, plan),
        "slots_per_input": {c: sum(1 for _, column in layout if column == c)
                            for c in [*plan.numeric, *plan.categorical, *plan.binary]},
        "probe": probe,
        "layout": [name for name, _ in layout],
        "config": asdict(config),
    }
    with open(ml_config.FEATURE_MANIFEST_PATH, "w") as handle:
        json.dump(manifest, handle, indent=2, default=str)

    print(json.dumps({k: manifest[k] for k in ("output", "plan", "learned", "slots_per_input", "probe")},
                     indent=2, default=str))
    written.show(3, truncate=100)
    print(f"fitted in {fit_seconds} s, wrote {config.path} in {write_seconds} s; pipeline in {config.model_path}; "
          f"manifest in {ml_config.FEATURE_MANIFEST_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
