import os


def _int_env(name, default):
	value = os.getenv(name)
	return int(value) if value is not None else default


DATA_PATH = os.getenv("DATA_PATH", "data/")
FEATURE_PATH = os.getenv("FEATURE_PATH", "features_store/")
MODEL_PATH = os.getenv("MODEL_PATH", "model/")

CHURN_WINDOW_DAYS = _int_env("CHURN_WINDOW_DAYS", 90)
N_PARTITIONS = _int_env("N_PARTITIONS", 200)
