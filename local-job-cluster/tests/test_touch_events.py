from touch_events.dedup import VARIANTS
from touch_events.generate import DUP_RATE, touch_events

N = 2_000


def test_redeliveries_are_about_ten_percent(spark):
    rows = touch_events(spark, N).count()
    assert N < rows < N * (1 + 2 * DUP_RATE)


def test_every_strategy_keeps_one_row_per_event(spark):
    events = touch_events(spark, N)
    for name, dedup in VARIANTS.items():
        assert dedup(events).count() == N, name


def test_redeliveries_differ_only_in_ingest_ts(spark):
    events = touch_events(spark, N)
    assert events.distinct().count() == events.count()
    assert events.drop("ingest_ts").distinct().count() == N


def test_window_keeps_the_latest_delivery(spark):
    events = touch_events(spark, N)
    latest = events.groupBy("event_id").agg({"ingest_ts": "max"}).toDF("event_id", "ingest_ts")
    kept = VARIANTS["window"](events).select("event_id", "ingest_ts")
    assert kept.subtract(latest).count() == 0
