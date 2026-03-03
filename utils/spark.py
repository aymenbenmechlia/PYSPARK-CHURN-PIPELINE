from pyspark.sql import SparkSession
import config

def get_spark(app_name="churn-pipeline"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(config.N_PARTITIONS))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
