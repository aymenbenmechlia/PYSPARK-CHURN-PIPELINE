from pyspark.sql.functions import count, avg, max

def build_usage_features(df_usage):
    return (
        df_usage.groupBy("customer_id")
        .agg(
            count("*").alias("nb_sessions"),
            avg("session_time").alias("avg_session_time"),
            max("event_date").alias("last_activity")
        )
    )
