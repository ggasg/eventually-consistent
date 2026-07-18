import org.apache.spark.sql.SparkSession

case class Event(id: Long, ts: Long, amount: Double, category: String)
case class Account(id: Long, region: String, tier: Int, signup_epoch: Long)

val RUNS = 10
val WARMUP = 2

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
