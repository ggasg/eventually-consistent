"""Writes a synthetic touch_events Delta table to MinIO. Argument: number of events."""
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PATH = "s3a://spark-experiments/touch_events"
DUP_RATE = 0.10
START_EPOCH = 1788220800  # 2026-09-01 00:00:00 UTC
WEEK_S = 7 * 86400

SURFACES = ["door_handle", "bed_rail", "iv_pump", "keyboard", "sink_tap", "light_switch"]
PATHOGENS = ["MRSA", "C_DIFF", "VRE", "E_COLI", "NOROVIRUS"]


def h(salt: int, n: int):
    """Deterministic pseudo-random long in [0, n) derived from the row id."""
    return F.pmod(F.xxhash64("id", F.lit(salt)), F.lit(n))


def pick(values: list[str], salt: int):
    return F.element_at(F.array(*[F.lit(v) for v in values]), (h(salt, len(values)) + 1).cast("int"))


def touch_events(spark: SparkSession, n_events: int) -> DataFrame:
    room = h(2, 2000)
    detected = h(5, 100) < 5
    events = spark.range(n_events).select(
        "id",
        F.md5(F.col("id").cast("string")).alias("event_id"),
        F.timestamp_seconds(F.lit(START_EPOCH) + h(1, WEEK_S)).alias("event_ts"),
        F.format_string("FAC-%02d", room % 20).alias("facility_id"),
        F.format_string("ROOM-%04d", room).alias("room_id"),
        pick(SURFACES, 3).alias("surface_type"),
        F.format_string("STAFF-%05d", h(4, 50_000)).alias("staff_id"),
        F.when(detected, pick(PATHOGENS, 6)).alias("pathogen_code"),
        F.when(detected, h(7, 500).cast("int")).alias("cfu_count"),
        (F.lit(START_EPOCH) + h(1, WEEK_S) + h(8, 5)).alias("ingest_epoch"),
    )
    # Redeliveries: same event, arriving 30s-10min after the original.
    redelivered = events.sample(fraction=DUP_RATE, seed=42).withColumn(
        "ingest_epoch", F.col("ingest_epoch") + 30 + h(9, 600)
    )
    return (
        events.unionByName(redelivered)
        .withColumn("ingest_ts", F.timestamp_seconds("ingest_epoch"))
        .drop("id", "ingest_epoch")
    )


def main() -> None:
    n_events = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000_000
    spark = SparkSession.builder.appName("generate-touch-events").getOrCreate()
    touch_events(spark, n_events).write.format("delta").mode("overwrite").save(PATH)
    print(f"rows written: {spark.read.format('delta').load(PATH).count():,}")


if __name__ == "__main__":
    main()
