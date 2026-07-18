from pyspark.sql import SparkSession

# Produces results/physical_plan.txt: prints the exact physical plan
# for a plain PySpark DataFrame join, with the same configs used by
# join_bench.py (forced SortMergeJoin, no AQE). This is the source of
# the "explain(\"formatted\") on both confirms..." claim in the article.
spark = (
    SparkSession.builder
    .master("local[4]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

events = spark.read.parquet("/tmp/bench/data/events")
accounts = spark.read.parquet("/tmp/bench/data/accounts")

joined = events.join(accounts, on="id", how="inner")
joined.explain("formatted")

spark.stop()
