from utils.spark import get_spark
from utils.io import read_parquet
from utils.preprocessing import validate_required_columns, validate_non_empty
import config
import json
import os

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

spark = get_spark("training")

df = read_parquet(spark, config.FEATURE_PATH)

validate_required_columns(df, ["customer_id", "churn"], "features_store")
validate_non_empty(df, "features_store")

feature_cols = [c for c in df.columns if c not in ["customer_id", "churn"]]

if not feature_cols:
    raise ValueError("No feature columns available for training")

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features",
    handleInvalid="skip"
)

lr = LogisticRegression(
    featuresCol="features",
    labelCol="churn",
    maxIter=20
)

pipeline = Pipeline(stages=[assembler, lr])
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
validate_non_empty(train_df, "train_split")
validate_non_empty(test_df, "test_split")

model = pipeline.fit(train_df)

pred_test = model.transform(test_df)

auc_eval = BinaryClassificationEvaluator(labelCol="churn", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
precision_eval = MulticlassClassificationEvaluator(labelCol="churn", predictionCol="prediction", metricName="weightedPrecision")
recall_eval = MulticlassClassificationEvaluator(labelCol="churn", predictionCol="prediction", metricName="weightedRecall")

metrics = {
    "auc": auc_eval.evaluate(pred_test),
    "weighted_precision": precision_eval.evaluate(pred_test),
    "weighted_recall": recall_eval.evaluate(pred_test),
}

os.makedirs(os.path.dirname(config.METRICS_PATH), exist_ok=True)
with open(config.METRICS_PATH, "w", encoding="utf-8") as f:
    json.dump(metrics, f, indent=2)

model.write().overwrite().save(config.MODEL_PATH)
