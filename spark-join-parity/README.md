# One Join, Two APIs, Same Plan

Companion code for [the article of the same name](https://ggasg.github.io/blog/one-join-two-apis-same-plan/): a controlled,
reproducible measurement of whether a Scala `Dataset.joinWith` and a PySpark
`DataFrame.join` differ in JVM resource consumption for the same
`SortMergeJoin`. The write-up lives on the blog; this repo is just the code,
tests, and raw results behind it.

## Layout

```
code/
  gen_data.py             generates the two synthetic Parquet tables
  join_bench.py           PySpark DataFrame join, 20 warmup + 50 measured runs
  JoinBench.scala         Scala Dataset joinWith, 20 warmup + 50 measured runs
  explain_plan.py         prints the PySpark physical plan (explain("formatted"))
  explain_plan.scala      prints the Scala physical plan (explain("formatted"))
  parse_events.py         parses Spark event logs into per-run metrics,
                          plus a Welch's t-test / 95% CI on the Scala-vs-
                          PySpark delta for each metric
  run_all.sh              runs the full pipeline end to end, single session
tests/
  test_parse_events.py    unit tests for the event log parser and the t-test
results/
  physical_plan.txt               explain("formatted") output for both jobs
  session_1_n10_warmup2.json      first pass: 10 runs, 2 warmup
  session_2_n50_warmup2.json      rerun: ~50 runs, still only 2 warmup
  session_2_n50_warmup2_raw.json  per-run values behind session 2
  session_3_n44_warmup20.json     rerun: 20 warmup before any measured run
  session_3_n44_warmup20_raw.json per-run values behind session 3
```

## Why three result files instead of one

The first pass (session 1) used 10 runs and 2 warmup iterations and reported
Scala about 8% slower on wall-clock duration. Rerunning with more measured
runs (session 2, still 2 warmup) reversed that: Scala came out faster, with a
p-value under 0.0001. Suspecting the 2 warmup iterations weren't enough for
the JIT to reach steady state, session 3 reran both engines with 20 warmup
iterations before measuring anything, in a separate session started later.
That reversed the result again, by an even larger margin, also at p < 0.0001.

Same code, same data, same JVM, three different answers about which engine
is faster. Across all three sessions, though, peak execution memory and
shuffle read/write bytes never moved by a single byte. That contrast, not
any one session's p-value, is the actual finding: the metrics the physical
plan determines are perfectly reproducible, and wall-clock duration, GC
time, and CPU time collected from separate sandboxed JVM sessions are not.
See the article for the full breakdown and what it does and doesn't
support.

## Reproducing

Requires a JDK (11+) and Python 3.9+.

```bash
pip install pyspark==3.5.1 scipy   # brings spark-shell and spark-submit with it
cd code
./run_all.sh
```

This writes synthetic data, runs both jobs locally with Spark event logging
turned on, parses the resulting event logs, and writes `results/summary.json`
plus `results/summary_raw.json`. `run_all.sh` runs one session; it will not
by itself reproduce the three-session comparison above.

To reproduce a second or third session the way this repo's results were
built, rerun the two benchmark scripts against a fresh event log directory
and give `parse_events.py` a distinct output name so it doesn't overwrite
the first session:

```bash
mkdir -p /tmp/bench/events_s2/pyspark /tmp/bench/events_s2/scala
# edit WARMUP in join_bench.py / JoinBench.scala if you want a different
# warmup count than the last run, then rerun both benchmark scripts with
# spark.eventLog.dir pointed at the new directory, then:
python3 code/parse_events.py --events-dir /tmp/bench/events_s2 --out-prefix session_2
```

`explain_plan.py` / `explain_plan.scala` aren't part of that pipeline, they're
a separate one-off check: same data, same configs, but just calling
`explain("formatted")` instead of running the timed loop, to confirm both
engines produce the identical physical plan regardless of which session's
timing numbers you're looking at. Run with `python3
code/explain_plan.py` and `spark-shell -i code/explain_plan.scala` (same
`--conf` flags as in `run_all.sh`) after the data exists.

## Running the tests

```bash
pip install scipy --break-system-packages   # or into a venv
python3 tests/test_parse_events.py
```

## Feedback

Corrections, replications on real (non-sandboxed) hardware, or a follow-up
that interleaves the two engines' runs to control for session-level noise
are welcome. Open an issue or a PR.
