# PySpark Customer Churn Pipeline

End-to-end churn prediction pipeline built with PySpark.

## Configuration

`config.py` supports environment variable overrides.

Start from `.env.example`:

cp .env.example .env

PowerShell example:

$env:DATA_PATH="data/"
$env:FEATURE_PATH="features_store/"
$env:MODEL_PATH="model/"
$env:METRICS_PATH="metrics/training_metrics.json"
$env:CHURN_WINDOW_DAYS="90"
$env:N_PARTITIONS="200"

## Run

Feature engineering:
spark-submit main_features.py

Training:
spark-submit main_train.py

Scoring:
spark-submit main_score.py
