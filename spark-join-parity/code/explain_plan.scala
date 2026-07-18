// Scala counterpart to explain_plan.py: prints the physical plan for
// the typed Dataset.joinWith version, same configs as JoinBench.scala.
// Run via spark-shell -i, with the same --conf flags used for the
// timed benchmark (see README.md).
case class Event(id: Long, ts: Long, amount: Double, category: String)
case class Account(id: Long, region: String, tier: Int, signup_epoch: Long)

import spark.implicits._

val events = spark.read.parquet("/tmp/bench/data/events").as[Event]
val accounts = spark.read.parquet("/tmp/bench/data/accounts").as[Account]
val joined = events.joinWith(accounts, events("id") === accounts("id"), "inner")
joined.explain("formatted")

System.exit(0)
