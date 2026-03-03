from pyspark.sql.functions import count

def build_support_features(df_support):
    return (
        df_support.groupBy("customer_id")
        .agg(count("*").alias("nb_tickets"))
    )
