# One Join, Two APIs, Zero Difference

Companion code for [the article of the same name](https://ggasg.github.io/blog/one-join-two-apis-zero-difference/): a controlled, reproducible
measurement of whether a Scala `Dataset.joinWith` and a PySpark
`DataFrame.join` differ in JVM resource consumption for the same
`SortMergeJoin`. The write-up lives on the blog; this repo is just the code,
tests, and raw results behind it.

## Layout

```
code/
  gen_data.py             generates the two synthetic Parquet tables
  join_bench.py           PySpark DataFrame join, 2 warmup + 10 measured runs
  JoinBench.scala         Scala Dataset joinWith, 2 warmup + 10 measured runs
  explain_plan.py         prints the PySpark physical plan (explain("formatted"))
  explain_plan.scala      prints the Scala physical plan (explain("formatted"))
  parse_events.py         parses Spark event logs into per-run metrics
  run_all.sh              runs the full pipeline end to end
results/
  summary.json            mean/stdev/min/max per metric, both engines
  physical_plan.txt        explain("formatted") output for both jobs
tests/
  test_parse_events.py    unit test for the event log parser
```

## Reproducing

Requires a JDK (11+) and Python 3.9+.

```bash
pip install pyspark==3.5.1   # brings spark-shell and spark-submit with it
cd code
./run_all.sh
```

This writes synthetic data, runs both jobs locally with Spark event logging
turned on, parses the resulting event logs, and writes `results/summary.json`.
Total runtime is a couple of minutes on a 4-core machine.

`explain_plan.py` / `explain_plan.scala` aren't part of that pipeline, they're
a separate one-off check: same data, same configs, but just calling
`explain("formatted")` instead of running the timed loop, to confirm both
engines produce the identical physical plan. Run with `python3
code/explain_plan.py` and `spark-shell -i code/explain_plan.scala` (same
`--conf` flags as in `run_all.sh`) after the data exists.

## Running the tests

```bash
python3 tests/test_parse_events.py
```

## Feedback

Corrections, replications on real hardware, or a follow-up run that adds a
UDF or a `.map`/`.rdd` step are welcome. Open an issue or a PR.
