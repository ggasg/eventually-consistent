from pyspark.sql import SparkSession

RUNS = 10
WARMUP = 2

spark = (
    SparkSession.builder
    .master("local[4]")
    .appName("pyspark-dataframe-join-bench")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "file:///tmp/bench/events/pyspark")
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
