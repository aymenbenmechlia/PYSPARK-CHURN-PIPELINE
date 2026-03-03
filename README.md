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

### Windows one-liner (PowerShell)

Run the full pipeline in one command (sets Python, Java and Hadoop env vars for the session):

$py='C:/Users/ben-m/AppData/Local/Programs/Python/Python311/python.exe'; $jdk=Get-ChildItem 'C:/Program Files/Eclipse Adoptium' -Directory | Sort-Object Name -Descending | Select-Object -First 1; $env:JAVA_HOME=$jdk.FullName; $env:HADOOP_HOME='C:/hadoop'; Set-Item -Path Env:hadoop.home.dir -Value 'C:/hadoop'; $env:PYSPARK_PYTHON=$py; $env:PYSPARK_DRIVER_PYTHON=$py; $env:Path="C:/Users/ben-m/AppData/Local/Programs/Python/Python311;C:/Users/ben-m/AppData/Local/Programs/Python/Python311/Scripts;$env:JAVA_HOME/bin;C:/hadoop/bin;$env:Path"; & $py main_features.py; & $py main_train.py; & $py main_score.py
