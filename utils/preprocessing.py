from pyspark.sql.functions import col

def clean_ids(df):
    return df.withColumn("customer_id", col("customer_id").cast("string"))

def drop_null_ids(df):
    return df.filter(col("customer_id").isNotNull())
