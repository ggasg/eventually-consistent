# Counting failing devices without keeping their names

Companion code for [the article of the same name](https://ggasg.github.io/blog/counting-failing-devices/).
The write-up lives on the blog; this repo is the deduplication step and the SQL
it feeds. The rest of the pipeline, the Pub/Sub reader and the threshold filter
and the BigQuery sink, is not published.

## Layout

```
code/
  device_id_windowing.py        the Beam CombineFn: one entry per device per window
  hll.py                        HyperLogLog++, standard library only
  sql/
    01_landing_table.sql        what the pipeline writes
    02_materialized_view.sql    HLL_COUNT.INIT, one sketch per hour per sensor type
    03_rollup_query.sql         HLL_COUNT.MERGE, any whole number of hours
tests/
  test_device_id_windowing.py   repeats collapse; state follows devices, not volume
  test_hll.py                   merging is associative and commutative
```

## Running the tests

```bash
pip install pytest apache-beam
python3 -m pytest tests -q
```

Beam is only needed for the CombineFn tests, which skip themselves when it is
not importable. `hll.py` and its tests run on a bare interpreter.

## On hll.py

BigQuery's `HLL_COUNT` functions are backed by ZetaSketch. `hll.py` is a short
implementation from the
[HyperLogLog++ paper](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf)
instead, without the sparse representation or the bias-correction tables, and it
does not emit BigQuery-compatible bytes. Its only job is to make the merge
properties assertable in a unit test.

## Feedback

If you have managed to build the sketches upstream in Beam and keep incremental
refresh on the materialized view, I would like to see how. Open an issue or a PR.
