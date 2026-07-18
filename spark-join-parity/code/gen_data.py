import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .master("local[4]")
    .appName("gen-data")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()

)
spark.sparkContext.setLogLevel("WARN")

ROWS = 5_000_000
KEY_RANGE = 2_500_000

# left / "events" table
left = (
    spark.range(0, ROWS, 1, 8)
    .withColumn("id", (F.rand(seed=42) * KEY_RANGE).cast("long"))
    .withColumn("ts", (F.rand(seed=7) * 1_700_000_000).cast("long"))
    .withColumn("amount", F.rand(seed=11) * 1000.0)
    .withColumn("category", F.element_at(
        F.array(*[F.lit(c) for c in ["A", "B", "C", "D", "E"]]),
        (F.rand(seed=13) * 5).cast("int") + 1
    ))
)
left.write.mode("overwrite").parquet("/tmp/bench/data/events")

# right / "accounts" table
right = (
    spark.range(0, ROWS, 1, 8)
    .withColumn("id", (F.rand(seed=99) * KEY_RANGE).cast("long"))
    .withColumn("region", F.element_at(
        F.array(*[F.lit(c) for c in ["us-east", "us-west", "eu", "apac"]]),
        (F.rand(seed=17) * 4).cast("int") + 1
    ))
    .withColumn("tier", (F.rand(seed=23) * 3).cast("int"))
    .withColumn("signup_epoch", (F.rand(seed=29) * 1_600_000_000).cast("long"))
)
right.write.mode("overwrite").parquet("/tmp/bench/data/accounts")

print("LEFT_COUNT", spark.read.parquet("/tmp/bench/data/events").count())
print("RIGHT_COUNT", spark.read.parquet("/tmp/bench/data/accounts").count())
spark.stop()
