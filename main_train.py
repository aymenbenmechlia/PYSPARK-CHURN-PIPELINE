from utils.spark import get_spark
from utils.io import read_parquet
import config

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

spark = get_spark("training")

df = read_parquet(spark, config.FEATURE_PATH)

feature_cols = [c for c in df.columns if c not in ["customer_id", "churn"]]

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
model = pipeline.fit(df)

model.write().overwrite().save(config.MODEL_PATH)
