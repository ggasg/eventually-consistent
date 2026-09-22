"""A small HyperLogLog++ implementation, standard library only.

Why this exists
---------------
The hourly rollup only works because two sketches can be merged into the one
you would have got by sketching both inputs together. That property is worth
asserting rather than taking on faith, and asserting it against BigQuery would
need a GCP project. This implementation makes it testable on a bare
interpreter.

What this is, precisely
-----------------------
The dense HyperLogLog++ register layout described in Heule, Nunkesser and
Hall, "HyperLogLog in Practice" (EDBT 2013), with a 64-bit hash and the
linear-counting correction for the small-cardinality range. It is *not*
ZetaSketch, the library behind BigQuery's HLL_COUNT functions, and it does
not emit ZetaSketch-compatible bytes. Two deliberate omissions:

  * the sparse representation, which changes sketch *size* at low
    cardinality but not the estimate;
  * the empirical bias-correction tables, which matter in the band between
    the linear-counting threshold and the point where the raw estimator is
    unbiased.

At the default precision of 15 the linear-counting branch covers every
cardinality the tests exercise (the threshold is 5m/2 = 81,920 distinct
values), so the omitted tables never come into play. The estimates are checked
against the error band BigQuery publishes for that precision.

Reference: https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
"""

from __future__ import annotations

import hashlib
import math

# BigQuery's HLL_COUNT.INIT default. Range 10 to 24.
# https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions
DEFAULT_PRECISION = 15

# ZetaSketch stores one byte per register once dense, which is where
# BigQuery's documented "32 KiB + 30 B" maximum at precision 15 comes from
# (2**15 bytes plus a header). https://cloud.google.com/bigquery/docs/sketches
DENSE_BYTES_PER_REGISTER = 1
SKETCH_HEADER_BYTES = 30

_HASH_BITS = 64
_MASK64 = (1 << _HASH_BITS) - 1


def hash64(value: str) -> int:
    """A stable 64-bit hash.

    Python's built-in hash() is salted per process, so the same device ID
    would land in a different register on every run and the tests would not
    be repeatable. SHA-256 truncated to 64 bits is slower and deterministic,
    which is the trade worth making here.
    """
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def _alpha(m: int) -> float:
    """The bias constant from the original HyperLogLog paper."""
    if m == 16:
        return 0.673
    if m == 32:
        return 0.697
    if m == 64:
        return 0.709
    return 0.7213 / (1.0 + 1.079 / m)


class HyperLogLogPlus:
    """A fixed-size cardinality sketch.

    The size never changes once allocated, which is the property the
    pipeline design is built on: memory is a function of precision, not of
    how many distinct devices turn up.
    """

    __slots__ = ("precision", "m", "registers", "_alpha_m")

    def __init__(self, precision: int = DEFAULT_PRECISION):
        if not 10 <= precision <= 24:
            raise ValueError(f"precision must be between 10 and 24, got {precision}")
        self.precision = precision
        self.m = 1 << precision
        self.registers = bytearray(self.m)
        self._alpha_m = _alpha(self.m)

    def add(self, value: str) -> None:
        h = hash64(value)
        index = h >> (_HASH_BITS - self.precision)
        remaining = (h << self.precision) & _MASK64
        rank = self._rank(remaining)
        if rank > self.registers[index]:
            self.registers[index] = rank

    def _rank(self, remaining: int) -> int:
        """Position of the leftmost 1-bit, 1-indexed, over the bits left
        after the register index has been taken off the top."""
        width = _HASH_BITS - self.precision
        if remaining == 0:
            return width + 1
        return _HASH_BITS - remaining.bit_length() + 1

    def merge(self, other: "HyperLogLogPlus") -> "HyperLogLogPlus":
        """Union two sketches. This is the whole point of a sketch: the
        operation is associative and commutative, so order and grouping of
        the inputs do not affect the result."""
        if other.precision != self.precision:
            raise ValueError("cannot merge sketches of different precision")
        merged = HyperLogLogPlus(self.precision)
        mine, theirs = self.registers, other.registers
        merged.registers = bytearray(
            a if a > b else b for a, b in zip(mine, theirs)
        )
        return merged

    def estimate(self) -> int:
        m = self.m
        zeros = self.registers.count(0)

        # Small-cardinality range: linear counting is more accurate than the
        # harmonic-mean estimator and has no bias to correct.
        if zeros:
            linear = m * math.log(m / zeros)
            if linear <= 2.5 * m:
                return int(round(linear))

        harmonic = 0.0
        for r in self.registers:
            harmonic += 2.0 ** -r
        raw = self._alpha_m * m * m / harmonic
        return int(round(raw))

    def serialized_size(self) -> int:
        """Bytes this sketch would occupy stored densely, matching the
        layout BigQuery documents for HLL_COUNT sketches."""
        return self.m * DENSE_BYTES_PER_REGISTER + SKETCH_HEADER_BYTES

    def __len__(self) -> int:
        return self.estimate()


def merge_all(sketches):
    """Merge an iterable of sketches into one. Returns None if empty."""
    result = None
    for sketch in sketches:
        result = sketch if result is None else result.merge(sketch)
    return result
