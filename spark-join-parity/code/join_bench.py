import os

from pyspark.sql import SparkSession

# Overridable so this same script can run as one half of a concurrent
# pair (see run_concurrent.sh): each half needs its own core budget,
# run count, event log directory, and app name, without duplicating
# this file.
RUNS = int(os.environ.get("BENCH_RUNS", "50"))
WARMUP = int(os.environ.get("BENCH_WARMUP", "20"))
MASTER = os.environ.get("BENCH_MASTER", "local[4]")
EVENT_LOG_DIR = os.environ.get("BENCH_EVENT_LOG_DIR", "file:///tmp/bench/events/pyspark")
APP_NAME = os.environ.get("BENCH_APP_NAME", "pyspark-dataframe-join-bench")
DRIVER_MEMORY = os.environ.get("BENCH_DRIVER_MEMORY", "2g")

spark = (
    SparkSession.builder
    .master(MASTER)
    .appName(APP_NAME)
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", DRIVER_MEMORY)
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", EVENT_LOG_DIR)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

events = spark.read.parquet("/tmp/bench/data/events")
accounts = spark.read.parquet("/tmp/bench/data/accounts")

for i in range(WARMUP + RUNS):
    tag = "warmup" if i < WARMUP else f"run_{i - WARMUP + 1}"
    sc.setJobGroup(tag, f"pyspark dataframe join - {tag}")
    joined = events.join(accounts, on="id", how="inner")
    total = joined.count()
    print(f"[{tag}] joined_count={total}")

sc.setJobGroup("", "")
spark.stop()
