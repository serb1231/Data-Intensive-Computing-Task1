import json
import time
from datetime import datetime

import ml_config
from feature_pipeline import FEATURES, feature_pipeline
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GBTRegressor
from training_dataset import latest_version, read_version
from data_ingestion import build_spark

MODEL_PATH = f"{ml_config.ML_DIR}/models/hourly_zone_demand_model"
MANIFEST_PATH = "model_week_4.json"
PARAMS = {"maxIter": 50, "maxDepth": 5, "seed": 42}


# some normally used metrics for machine learning
def score(model, split_data, target):
    # the usual regression metrics of the fitted pipeline on one split.
    predictions = model.transform(split_data)
    return {metric: round(RegressionEvaluator(labelCol=target, metricName=metric).evaluate(predictions), 4)
            for metric in ("rmse", "mae", "r2")}


def main():
    config = ml_config.TRAINING_DATASET
    target = config.target

    spark = build_spark()
    # in order to see only fatal errors
    spark.sparkContext.setLogLevel("ERROR")

    version = latest_version(spark, config.path)
    dataset = read_version(spark, config.path, version)
    # cache the result as it takes too long
    train = dataset.where("split = 'train'").cache()

    # the Task 2 stages
    started = time.time()
    features, plan = feature_pipeline(train)
    # fir the regressor on the train-test split onyl
    regressor = GBTRegressor(featuresCol=FEATURES, labelCol=target, **PARAMS)
    model = Pipeline(stages=features.getStages() + [regressor]).fit(train)
    fit_seconds = round(time.time() - started, 2)
    model.write().overwrite().save(MODEL_PATH)

    scores = {split: score(model, dataset.where(f"split = '{split}'"), target)
              for split in ("train", "validation", "test")}
    # save the metadata for later
    manifest = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "fit_seconds": fit_seconds,
        "dataset": {"path": config.path, "delta_version": version, "target": target},
        "model": {"path": MODEL_PATH, "regressor": "GBTRegressor", "params": PARAMS,
                  "feature_count": model.stages[-1].numFeatures},
        "dropped_features": plan.dropped,
        "scores": scores,
    }
    with open(MANIFEST_PATH, "w") as handle:
        json.dump(manifest, handle, indent=2, default=str)

    print(json.dumps(manifest, indent=2, default=str))
    print(f"fitted in {fit_seconds} s on {config.path} v{version}; model in {MODEL_PATH}; "
          f"manifest in {MANIFEST_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
