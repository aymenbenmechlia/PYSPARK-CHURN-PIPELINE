from pyspark.sql.functions import sum as _sum, avg

def build_billing_features(df_billing):
    return (
        df_billing.groupBy("customer_id")
        .agg(
            _sum("amount").alias("total_spent"),
            avg("amount").alias("avg_invoice")
        )
    )
