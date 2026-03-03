from utils.spark import get_spark
from utils.io import read_parquet, write_parquet
import config

from pyspark.ml import PipelineModel

spark = get_spark("scoring")

df = read_parquet(spark, config.FEATURE_PATH)
model = PipelineModel.load(config.MODEL_PATH)

pred = model.transform(df)

write_parquet(pred.select("customer_id", "probability"), "predictions/")
