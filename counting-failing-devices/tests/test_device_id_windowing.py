"""Unit tests for the per-window deduplication.

These do not need a Beam runner. A CombineFn is an ordinary object, so the
four methods can be exercised directly, which is enough to pin the behaviour
the article describes: repeats collapse, and the accumulator's size follows
distinct devices rather than message volume.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "code"))

import pytest

# importorskip only catches ImportError, and a half-installed beam can fail
# with something else entirely. Skipping on any failure keeps a broken
# environment from turning into a collection error.
try:
    from device_id_windowing import DistinctDeviceIdsCombineFn
except Exception as exc:  # pragma: no cover
    pytest.skip(f"apache-beam unavailable: {exc}", allow_module_level=True)


@pytest.fixture
def fn():
    return DistinctDeviceIdsCombineFn()


def combine(fn, device_ids):
    acc = fn.create_accumulator()
    for device_id in device_ids:
        acc = fn.add_input(acc, device_id)
    return fn.extract_output(acc)


def test_an_empty_window_emits_nothing(fn):
    assert combine(fn, []) == []


def test_repeats_collapse_to_one_entry(fn):
    assert combine(fn, ["d1"] * 50) == ["d1"]


def test_distinct_devices_are_all_kept(fn):
    assert combine(fn, ["d1", "d2", "d1", "d3", "d2"]) == ["d1", "d2", "d3"]


def test_output_is_ordered_so_rows_are_stable(fn):
    assert combine(fn, ["d3", "d1", "d2"]) == combine(fn, ["d2", "d3", "d1"])


def test_accumulator_size_follows_devices_not_message_volume(fn):
    """The property the design rests on: a hundred times the readings from the
    same devices leaves the same number of entries in memory."""
    quiet = fn.create_accumulator()
    for device_id in [f"d{i}" for i in range(100)]:
        quiet = fn.add_input(quiet, device_id)

    noisy = fn.create_accumulator()
    for device_id in [f"d{i}" for i in range(100)] * 100:
        noisy = fn.add_input(noisy, device_id)

    assert len(quiet) == len(noisy) == 100


def test_ids_are_coerced_to_strings(fn):
    """Device IDs arrive off a JSON payload and are not always strings. The
    landing table column is ARRAY<STRING>, so they are normalised here."""
    assert combine(fn, [1, "1"]) == ["1"]


def test_falsy_ids_are_dropped(fn):
    assert combine(fn, ["d1", None, "", "d2"]) == ["d1", "d2"]


def test_merging_accumulators_unions_them(fn):
    """Beam merges partial accumulators when it combines across bundles."""
    a = fn.add_input(fn.create_accumulator(), "d1")
    b = fn.add_input(fn.create_accumulator(), "d2")
    c = fn.add_input(fn.create_accumulator(), "d1")
    assert fn.extract_output(fn.merge_accumulators([a, b, c])) == ["d1", "d2"]


def test_merging_nothing_gives_an_empty_window(fn):
    assert fn.extract_output(fn.merge_accumulators([])) == []
