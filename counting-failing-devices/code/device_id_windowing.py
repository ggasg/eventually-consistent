"""Per-window deduplication of device IDs.

The accumulator is a set, so a device that breaches its threshold fifty times
inside one minute contributes a single entry. What leaves the window scales
with the number of distinct devices in alarm, not with the message rate.

The sketch is deliberately not built here. See code/sql/02_materialized_view.sql.
"""

import apache_beam as beam


class DistinctDeviceIdsCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return set()

    def add_input(self, accumulator, device_id):
        if device_id:
            accumulator.add(str(device_id))
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = set()
        for acc in accumulators:
            merged.update(acc)
        return merged

    def extract_output(self, accumulator):
        return sorted(accumulator)
