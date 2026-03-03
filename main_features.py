from utils.spark import get_spark
from utils.io import read_parquet, write_parquet
from utils.preprocessing import (
    clean_ids,
    drop_null_ids,
    validate_required_columns,
    validate_non_empty,
)

from features.usage_features import build_usage_features
from features.billing_features import build_billing_features
from features.support_features import build_support_features

import config

spark = get_spark("feature-engineering")

df_usage = read_parquet(spark, config.DATA_PATH + "usage/")
df_billing = read_parquet(spark, config.DATA_PATH + "billing/")
df_support = read_parquet(spark, config.DATA_PATH + "support/")
df_crm = read_parquet(spark, config.DATA_PATH + "crm/")

df_usage = drop_null_ids(clean_ids(df_usage))
df_billing = drop_null_ids(clean_ids(df_billing))
df_support = drop_null_ids(clean_ids(df_support))
df_crm = drop_null_ids(clean_ids(df_crm))

validate_required_columns(df_usage, ["customer_id", "session_time", "event_date"], "usage")
validate_required_columns(df_billing, ["customer_id", "amount"], "billing")
validate_required_columns(df_support, ["customer_id"], "support")
validate_required_columns(df_crm, ["customer_id", "churn"], "crm")

validate_non_empty(df_usage, "usage")
validate_non_empty(df_billing, "billing")
validate_non_empty(df_support, "support")
validate_non_empty(df_crm, "crm")

f_usage = build_usage_features(df_usage)
f_billing = build_billing_features(df_billing)
f_support = build_support_features(df_support)

df_features = (
    f_usage
    .join(f_billing, "customer_id", "left")
    .join(f_support, "customer_id", "left")
    .join(df_crm, "customer_id", "left")
)

write_parquet(df_features, config.FEATURE_PATH)
