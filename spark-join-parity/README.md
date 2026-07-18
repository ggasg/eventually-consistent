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
  join_bench.py           PySpark DataFrame join. RUNS/WARMUP and, for the
                          concurrent harness, MASTER/EVENT_LOG_DIR/APP_NAME/
                          DRIVER_MEMORY are all overridable via BENCH_* env vars
  JoinBench.scala         Scala Dataset joinWith, same RUNS/WARMUP override
  explain_plan.py         prints the PySpark physical plan (explain("formatted"))
  explain_plan.scala      prints the Scala physical plan (explain("formatted"))
  parse_events.py         parses Spark event logs into per-run metrics; runs
                          a Welch's t-test (unpaired) and, with --paired, a
                          paired t-test on runs matched by wall-clock timestamp
  run_all.sh              single session, one engine after the other
  run_concurrent.sh       both engines launched at the same instant, local[2]
                          each, so a paired analysis can control for shared
                          ambient noise instead of being confounded by it
tests/
  test_parse_events.py    unit tests for the parser and both t-tests
results/
  physical_plan.txt                explain("formatted") output for both jobs
  session_1_n10_warmup2.json       first pass: 10 runs, 2 warmup, sequential
  session_2_n50_warmup2.json       rerun: ~50 runs, still 2 warmup, sequential
  session_3_n44_warmup20.json      rerun: 20 warmup, sequential, separate session
  session_4a/4b/4c_concurrent_n15  three independent concurrent+paired sessions
  *_raw.json                       per-run values behind each session above
```

## What this repo's history actually shows

This benchmark was run seven times, trying progressively more rigorous
designs, and the progression itself is the most honest thing to publish
here.

**Sessions 1-3** ran each engine sequentially: PySpark's entire sample, then
Scala's, in separate process invocations. Session 1 (10 runs, 2 warmup)
found Scala 8% slower on duration. Session 2 (about 50 runs, still 2 warmup)
reversed that at p < 0.0001. Session 3 (20 warmup iterations, to rule out
insufficient JIT warm-up) reversed it again, by a wider margin, also
significant. Same code, same data, three different answers.

**Sessions 4a-4c** launch both engines at the same instant instead (see
`run_concurrent.sh`), local[2] each, and pair each PySpark run with the
closest-in-time Scala run for a paired t-test instead of comparing two
separate session means (see `pair_by_time` / `paired_t_test` in
`parse_events.py`). This is a strictly better design; it removes the
"whole sample came from an earlier time window" confound that sessions 1-3
had. Sessions 4a and 4b agreed with each other: no significant difference in
duration, Scala consistently 3-4% higher on CPU time. Session 4c, run the
same way minutes later, disagreed with both: duration swung to Scala 232ms
*faster* (p = 0.0004), and CPU time flipped sign entirely.

Across all seven sessions, without exception, peak execution memory and
shuffle read/write bytes never moved by a single byte. Every timing metric
did, in both directions, repeatedly, regardless of how carefully the
comparison was designed.

**The conclusion this points to:** the noise isn't primarily about *when*
each engine's sample was collected relative to the other (sessions 4a-4c
controlled for that and still disagreed with each other). It's the sandbox
itself, a shared, virtualized, resource-constrained environment not built
for latency-sensitive measurement. Getting a trustworthy answer on timing
needs to happen on dedicated hardware, not here.

## Running it on dedicated hardware

Requires a JDK (11+), Python 3.9+, and `pip install pyspark==3.5.1 scipy`.

```bash
cd code
python3 gen_data.py
```

Then use `run_concurrent.sh`, not `run_all.sh`: it's the design that
controls for shared time-varying noise, which matters even more on a
machine you don't fully control (laptop under other load, cloud VM, etc.)
than it does on dedicated bare metal, and costs nothing extra to use if
your hardware turns out to be quiet.

```bash
BENCH_RUNS=50 BENCH_WARMUP=20 ./run_concurrent.sh /tmp/bench_concurrent
python3 parse_events.py --events-dir /tmp/bench_concurrent/events \
  --out-prefix local_run --paired
```

`BENCH_RUNS` / `BENCH_WARMUP` default to 50/20 if omitted, that's the
number that had no trouble completing in the sandbox sessions above once
CPU contention wasn't cutting it off mid-run; there's no reason to use
fewer on a real machine. Read the paired section of the output (or
`results/local_run.json` -> `paired_delta_scala_minus_pyspark`), not the
unpaired Welch's section, the whole point of this design is that the
paired test controls for noise the unpaired one doesn't.

`run_all.sh` still exists for reproducing the single-session numbers in
`session_1`/`session_2`/`session_3` exactly as they were originally
produced, but it's the design that turned out to be unreliable; don't use
it to generate new numbers to draw conclusions from.

`explain_plan.py` / `explain_plan.scala` aren't part of either pipeline,
they're a separate one-off check: same data, same configs, but just calling
`explain("formatted")` instead of running the timed loop, to confirm both
engines produce the identical physical plan regardless of what any
session's timing numbers show. Run with `python3 explain_plan.py` and
`spark-shell -i explain_plan.scala` (same `--conf` flags as in
`run_all.sh`) after the data exists.

## Running the tests

```bash
pip install scipy --break-system-packages   # or into a venv
python3 tests/test_parse_events.py
```

## Feedback

Corrections, replications on real hardware, or a look at whether a longer
warm-up or a different core split changes the concurrent-session picture
are all welcome. Open an issue or a PR.
