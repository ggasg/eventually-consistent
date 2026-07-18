import org.apache.spark.sql.SparkSession

case class Event(id: Long, ts: Long, amount: Double, category: String)
case class Account(id: Long, region: String, tier: Int, signup_epoch: Long)

// Overridable via env vars so the same script can run with a shorter
// schedule as one half of a concurrent pair (see run_concurrent.sh),
// without duplicating this file.
val RUNS = sys.env.getOrElse("BENCH_RUNS", "50").toInt
val WARMUP = sys.env.getOrElse("BENCH_WARMUP", "20").toInt

import spark.implicits._

val events = spark.read.parquet("/tmp/bench/data/events").as[Event]
val accounts = spark.read.parquet("/tmp/bench/data/accounts").as[Account]

for (i <- 0 until (WARMUP + RUNS)) {
  val tag = if (i < WARMUP) "warmup" else s"run_${i - WARMUP + 1}"
  sc.setJobGroup(tag, s"scala dataset joinWith - $tag")
  val joined = events.joinWith(accounts, events("id") === accounts("id"), "inner")
  val total = joined.count()
  println(s"[$tag] joined_count=$total")
}

sc.setJobGroup("", "")
System.exit(0)
