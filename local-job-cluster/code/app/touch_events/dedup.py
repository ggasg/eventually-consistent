"""Deduplicates touch_events with one strategy. Argument: distinct, drop_duplicates or window."""
import sys
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

PATH = "s3a://spark-experiments/touch_events"


def distinct(df: DataFrame) -> DataFrame:
    # ingest_ts records when a copy arrived, so it is left out of the comparison.
    return df.drop("ingest_ts").distinct()


def drop_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["event_id"])


def window(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("event_id").orderBy(F.col("ingest_ts").desc())
    return df.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")


VARIANTS = {f.__name__: f for f in (distinct, drop_duplicates, window)}


def main() -> None:
    variant = sys.argv[1]
    spark = SparkSession.builder.appName(f"dedup-{variant}").getOrCreate()
    result = VARIANTS[variant](spark.read.format("delta").load(PATH))
    start = time.perf_counter()
    result.write.format("noop").mode("overwrite").save()
    print(f"{variant}: {time.perf_counter() - start:.1f}s")


if __name__ == "__main__":
    main()
