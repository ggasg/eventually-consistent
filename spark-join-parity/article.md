## One Join, Two APIs, Zero Difference: Scala Datasets vs PySpark DataFrames on the JVM

Ask around and you'll hear it as settled fact: Scala beats PySpark, and a Scala Dataset join will always outrun the same join written as a PySpark DataFrame. It gets repeated often enough that almost nobody checks it where it would actually show up, inside the JVM, at the level of the physical operator. So here it is, checked, with numbers.

### What's actually being compared

A Spark join, whether triggered from `df.join(...)` in PySpark or `ds.joinWith(...)` in Scala, doesn't run as Python or Scala at execution time. Both go through the same path: your call builds a logical plan, Catalyst optimizes it, and the result is a physical plan made of operators (`Exchange`, `Sort`, `SortMergeJoin`, `Project`, and so on) that get compiled to JVM bytecode via whole-stage code generation. Python only re-enters the picture if you attach a Python UDF or pull rows back into the driver as Python objects. A plain DataFrame join does neither.

The Dataset API adds one more wrinkle worth being precise about. `Dataset[T].joinWith` is typed: it returns `Dataset[(A, B)]` instead of a `DataFrame`, using Spark's encoders to describe `A` and `B`. The encoder step exists to convert between the JVM's internal `UnsafeRow` format and your case class when you actually touch the typed objects (via `.map`, `.collect`, `.foreach`, etc.). If you don't touch them, and instead go straight to `.count()`, Catalyst doesn't materialize any case class instances at all. Which is why this is worth measuring instead of assuming.

This isn't a Scala-versus-Python comparison. It's narrower: does the typed `Dataset.joinWith` API change what the join operator does on the JVM, in memory, CPU, and shuffle I/O, compared to the untyped `DataFrame.join`?

### Setup

- Apache Spark 3.5.1, Scala 2.12.18, Python 3.10.12, OpenJDK 11
- Single machine, 4 cores, `local[4]`, 3.8 GB RAM, 2 GB driver heap
- Two synthetic Parquet tables, 5,000,000 rows each, join keys drawn from a shared range of 2,500,000 values (average fan-out of 2 on each side), producing roughly 10,000,000 joined rows
- `spark.sql.autoBroadcastJoinThreshold = -1` and `spark.sql.adaptive.enabled = false`, to force a deterministic `SortMergeJoin` in both cases rather than let size-based heuristics or AQE pick different plans between runs
- `spark.sql.shuffle.partitions = 8`

PySpark side:

```python
joined = events.join(accounts, on="id", how="inner")
total = joined.count()
```

Scala side:

```scala
case class Event(id: Long, ts: Long, amount: Double, category: String)
case class Account(id: Long, region: String, tier: Int, signup_epoch: Long)

val events   = spark.read.parquet(eventsPath).as[Event]
val accounts = spark.read.parquet(accountsPath).as[Account]
val joined   = events.joinWith(accounts, events("id") === accounts("id"), "inner")
val total    = joined.count()
```

`explain("formatted")` on both confirms they compile to the identical physical plan:

```
* Project (12)
+- * SortMergeJoin Inner (11)
   :- * Sort (5)
   :  +- Exchange (4)
   :     +- * Filter (3)
   :        +- * ColumnarToRow (2)
   :           +- Scan parquet  (1)
   +- * Sort (10)
      +- Exchange (9)
         +- * Filter (8)
            +- * ColumnarToRow (7)
               +- Scan parquet  (6)
```

No `DeserializeToObject` node shows up on the Scala side. Because the query never consumes the typed tuple (just a `count`), Catalyst never needs to build `(Event, Account)` instances, so the "Dataset" part of Dataset never actually executes.

### How the numbers were collected

Each run is tagged with `sc.setJobGroup(...)` before the action, and Spark's own event log (`spark.eventLog.enabled=true`) is parsed afterward for `SparkListenerTaskEnd` metrics on every task in that job group: JVM GC time, executor CPU time, peak execution memory, shuffle read/write bytes. These are the numbers Spark records internally, not a stopwatch estimate of them. Each engine ran 2 warmup iterations (discarded, to let the JIT settle) followed by 10 measured iterations in the same JVM/session. The parser and its aggregation logic are in `code/parse_events.py`, with a unit test in `tests/test_parse_events.py` against a small synthetic event log.

### Results (mean ± stdev over 10 runs)

| Metric | PySpark DataFrame | Scala Dataset | Delta |
|---|---|---|---|
| Duration | 789.5 ms ± 55.9 | 853.5 ms ± 32.4 | +8.1% |
| JVM GC time | 84.8 ms ± 17.3 | 82.0 ms ± 11.5 | -3.3% |
| Executor CPU time | 1997.7 ms ± 24.1 | 2015.5 ms ± 22.0 | +0.9% |
| Peak execution memory | 96.00 MB ± 0.0 | 96.00 MB ± 0.0 | 0.0% |
| Shuffle read + write | 100.87 MB ± 0.0 | 100.87 MB ± 0.0 | 0.0% |

Peak execution memory and shuffle I/O are identical to the byte, across every one of the 20 measured runs, not just close. That's expected: both are determined entirely by the plan and the partitioning, which are the same plan. CPU time is within 1%, well inside run-to-run noise. GC time is within its own stdev band in both directions. Wall-clock duration shows an 8% gap in favor of PySpark, but the two stdev bands overlap heavily (PySpark's own run-to-run spread is 55ms, larger than the 64ms gap between the means), so this reads as scheduling noise on a shared 4-core sandbox rather than a property of either API. Wall time alone is a noisy proxy for what an operator costs. That's why this piece measured GC time, CPU time, memory, and shuffle I/O directly, instead of stopping at which one finished first.

### Where this stops being true

This result is specific to a plain DataFrame/Dataset join with no UDFs and no typed consumption of the join output. The comparison would look different, and probably should, in a few cases this benchmark deliberately didn't touch:

A Python UDF inside the join's projection forces row-by-row serialization across the Py4J or Arrow boundary into a separate Python process, which is real, measurable overhead that has nothing to do with the join operator and everything to do with leaving the JVM. Calling `.rdd` or `.map` on the Dataset side forces the encoder to actually build case class instances, which costs something too, just not inside `SortMergeJoin`. And this was single-machine local mode; a real shuffle across a network in a multi-node cluster introduces its own variables (serialization codec, network bandwidth) that don't exist here. Any of these would make a reasonable follow-up.

### Takeaway

For a plain DataFrame-style join with no UDFs, this data doesn't support "Scala is faster" at the operator level. The join runs as the same compiled plan doing the same JVM work regardless of which API built it. The language choice matters for what happens around the join, not inside it.

All code, the raw event logs' parsed summary, and the physical plan output are in this repo under `code/` and `results/`. Corrections, replications on real hardware, or a follow-up run that adds a UDF or a `.map` step are welcome.

### References

- Armbrust, M., Xin, R. S., Lian, C., Huai, Y., Liu, D., Bradley, J. K., Meng, X., Kaftan, T., Franklin, M. J., Ghodsi, A., Zaharia, M. ["Spark SQL: Relational Data Processing in Spark."](https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf) SIGMOD 2015. The Catalyst optimizer that turns both `df.join` and `ds.joinWith` into the same physical plan.
- Xin, R., Rosen, J. ["Project Tungsten: Bringing Apache Spark Closer to Bare Metal."](https://www.databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html) Databricks blog, 2015.
- Agarwal, S., Liu, D., Xin, R. ["Apache Spark as a Compiler: Joining a Billion Rows per Second on a Laptop."](https://www.databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html) Databricks blog, 2016. Whole-stage code generation, specifically for joins.
