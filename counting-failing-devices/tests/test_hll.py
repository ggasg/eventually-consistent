"""Unit tests for the sketch implementation.

Merging is associative and commutative, which is the only reason an hourly
rollup can be re-aggregated into any other time grain, so that property is
asserted here rather than assumed. The estimate is also checked against the
error band BigQuery publishes for the default precision.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "code"))

import pytest

from hll import (
    DEFAULT_PRECISION,
    DENSE_BYTES_PER_REGISTER,
    SKETCH_HEADER_BYTES,
    HyperLogLogPlus,
    hash64,
    merge_all,
)

# BigQuery documents +/-1.72% at 99% confidence for precision 15.
# https://cloud.google.com/bigquery/docs/sketches
PUBLISHED_99_CI_PRECISION_15 = 0.0172


def sketch_of(values):
    sketch = HyperLogLogPlus()
    for value in values:
        sketch.add(value)
    return sketch


def ids(start, stop):
    return [f"device-{i:07d}" for i in range(start, stop)]


def test_hash_is_stable_across_processes():
    # Python's built-in hash() is salted per process. Switching to it would
    # make these assertions pass or fail depending on the interpreter run.
    assert hash64("device-0000001") == hash64("device-0000001")
    assert hash64("device-0000001") != hash64("device-0000002")


def test_empty_sketch_estimates_zero():
    assert HyperLogLogPlus().estimate() == 0


def test_small_cardinality_is_close_but_still_not_exact():
    """Worth being precise about, because it is easy to assume otherwise.

    Below the linear-counting threshold the estimator is very accurate, and
    it is still an estimator. At five hundred distinct devices this sketch
    answers 501. There is no cardinality at which HLL++ becomes exact, which
    is the whole reason it cannot be used for billing.
    """
    sketch = sketch_of(ids(0, 500))
    assert sketch.estimate() == 501
    assert abs(sketch.estimate() - 500) / 500 < 0.005


@pytest.mark.parametrize("cardinality", [100, 1_000, 10_000, 50_000, 80_000])
def test_error_within_published_band(cardinality):
    estimate = sketch_of(ids(0, cardinality)).estimate()
    relative_error = abs(estimate - cardinality) / cardinality
    assert relative_error <= PUBLISHED_99_CI_PRECISION_15, (
        f"{cardinality}: estimated {estimate}, error {relative_error:.4%}"
    )


def test_duplicates_do_not_change_the_estimate():
    once = sketch_of(ids(0, 5_000))
    forty_times = sketch_of(ids(0, 5_000) * 40)
    assert once.estimate() == forty_times.estimate()


def test_merge_is_commutative():
    a, b = sketch_of(ids(0, 3_000)), sketch_of(ids(2_000, 6_000))
    assert a.merge(b).estimate() == b.merge(a).estimate()


def test_merge_is_associative():
    a, b, c = sketch_of(ids(0, 2_000)), sketch_of(ids(1_000, 4_000)), sketch_of(ids(3_000, 7_000))
    assert a.merge(b).merge(c).estimate() == a.merge(b.merge(c)).estimate()


def test_merge_matches_counting_the_union_directly():
    """The property the hourly rollup depends on: merging per-hour sketches
    gives the same answer as sketching the whole range at once."""
    hours = [sketch_of(ids(h * 900, h * 900 + 1_500)) for h in range(24)]
    merged = merge_all(hours)
    direct = sketch_of(ids(0, 23 * 900 + 1_500))
    assert merged.estimate() == direct.estimate()


def test_merge_does_not_mutate_its_inputs():
    a, b = sketch_of(ids(0, 1_000)), sketch_of(ids(5_000, 6_000))
    before = a.estimate()
    a.merge(b)
    assert a.estimate() == before


def test_merge_rejects_mismatched_precision():
    with pytest.raises(ValueError):
        HyperLogLogPlus(14).merge(HyperLogLogPlus(15))


def test_size_is_independent_of_cardinality():
    """The property the design is sold on: the summary never grows."""
    small = sketch_of(ids(0, 10))
    large = sketch_of(ids(0, 90_000))
    assert small.serialized_size() == large.serialized_size()


def test_dense_size_matches_bigquery_documented_maximum():
    # BigQuery documents 32 KiB + 30 B at precision 15.
    expected = (1 << DEFAULT_PRECISION) * DENSE_BYTES_PER_REGISTER + SKETCH_HEADER_BYTES
    assert HyperLogLogPlus().serialized_size() == expected == 32 * 1024 + 30


@pytest.mark.parametrize("precision", [9, 25, 0, -1])
def test_precision_outside_the_supported_range_is_rejected(precision):
    with pytest.raises(ValueError):
        HyperLogLogPlus(precision)


def test_merge_all_of_nothing_is_none():
    assert merge_all([]) is None
