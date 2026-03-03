from pyspark.sql.functions import col


def clean_ids(df):
    return df.withColumn("customer_id", col("customer_id").cast("string"))


def drop_null_ids(df):
    return df.filter(col("customer_id").isNotNull())


def validate_required_columns(df, required_columns, dataset_name):
    missing = sorted(set(required_columns) - set(df.columns))
    if missing:
        missing_str = ", ".join(missing)
        raise ValueError(f"Missing required columns in {dataset_name}: {missing_str}")


def validate_non_empty(df, dataset_name):
    if df.limit(1).count() == 0:
        raise ValueError(f"Dataset is empty after preprocessing: {dataset_name}")
