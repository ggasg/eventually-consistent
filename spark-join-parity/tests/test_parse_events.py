"""
Unit test for code/parse_events.py against a small synthetic event log,
so the aggregation logic (job group -> stages -> task metrics) is
verified independently of a real Spark run.
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "code"))
import parse_events as pe


def _job_start(job_id, group, sub_time, stage_ids):
    return {
        "Event": "SparkListenerJobStart",
        "Job ID": job_id,
        "Submission Time": sub_time,
        "Properties": {"spark.jobGroup.id": group},
        "Stage Infos": [{"Stage ID": sid} for sid in stage_ids],
    }


def _job_end(job_id, comp_time):
    return {"Event": "SparkListenerJobEnd", "Job ID": job_id, "Completion Time": comp_time}


def _task_end(stage_id, gc_ms, cpu_ns, peak_mem_bytes, shuffle_read, shuffle_write):
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Task Metrics": {
            "JVM GC Time": gc_ms,
            "Executor Run Time": gc_ms * 5,
            "Executor CPU Time": cpu_ns,
            "Peak Execution Memory": peak_mem_bytes,
            "Memory Bytes Spilled": 0,
            "Disk Bytes Spilled": 0,
            "Shuffle Read Metrics": {"Local Bytes Read": shuffle_read, "Remote Bytes Read": 0},
            "Shuffle Write Metrics": {"Shuffle Bytes Written": shuffle_write},
        },
    }


def build_fixture():
    events = [
        _job_start(0, "run_1", 1000, [0]),
        _task_end(0, gc_ms=10, cpu_ns=100_000_000, peak_mem_bytes=1024 * 1024, shuffle_read=2048, shuffle_write=4096),
        _task_end(0, gc_ms=20, cpu_ns=200_000_000, peak_mem_bytes=2 * 1024 * 1024, shuffle_read=2048, shuffle_write=4096),
        _job_end(0, 1500),
        _job_start(1, "run_2", 2000, [1]),
        _task_end(1, gc_ms=5, cpu_ns=50_000_000, peak_mem_bytes=1024 * 1024, shuffle_read=1024, shuffle_write=1024),
        _job_end(1, 2300),
    ]
    return "\n".join(json.dumps(e) for e in events)


def test_parse_log_aggregates_correctly():
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        f.write(build_fixture())
        path = f.name

    try:
        results = pe.parse_log(path)
    finally:
        os.unlink(path)

    assert set(results.keys()) == {"run_1", "run_2"}

    r1 = results["run_1"]
    assert r1["n_tasks"] == 2
    assert r1["duration_ms"] == 500  # 1500 - 1000
    assert r1["jvm_gc_time_ms"] == 30  # 10 + 20
    assert abs(r1["peak_execution_memory_max_mb"] - 2.0) < 1e-6  # max(1MB, 2MB)
    assert abs(r1["shuffle_read_mb"] - (4096 / (1024 * 1024))) < 1e-6
    assert abs(r1["shuffle_write_mb"] - (8192 / (1024 * 1024))) < 1e-6

    r2 = results["run_2"]
    assert r2["n_tasks"] == 1
    assert r2["duration_ms"] == 300  # 2300 - 2000
    assert r2["jvm_gc_time_ms"] == 5


def test_peak_memory_is_max_not_sum_across_tasks():
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        f.write(build_fixture())
        path = f.name
    try:
        results = pe.parse_log(path)
    finally:
        os.unlink(path)

    r1 = results["run_1"]
    # two tasks with peak memory 1MB and 2MB -> max should be 2MB, not 3MB
    assert r1["peak_execution_memory_max_mb"] == 2.0


def test_welch_t_test_detects_a_real_separation():
    # two clearly separated samples, no overlap: should read as significant
    pyspark_vals = [100, 102, 98, 101, 99, 100, 103, 97, 101, 99]
    scala_vals = [130, 128, 132, 129, 131, 127, 133, 130, 129, 131]

    result = pe.welch_t_test(pyspark_vals, scala_vals)

    assert result["p_value"] is not None
    assert result["p_value"] < 0.01
    assert result["mean_diff"] > 25  # scala - pyspark, matches Delta's sign
    lo, hi = result["ci_95"]
    assert lo > 0  # 0 is not inside the CI when the effect is real


def test_welch_t_test_does_not_flag_overlapping_noise():
    # same generating process, run-to-run jitter only: should not read as significant
    pyspark_vals = [700, 950, 780, 820, 690, 910, 760, 840, 700, 950]
    scala_vals = [740, 900, 800, 830, 720, 880, 790, 810, 760, 920]

    result = pe.welch_t_test(pyspark_vals, scala_vals)

    assert result["p_value"] is not None
    lo, hi = result["ci_95"]
    assert lo < 0 < hi  # 0 falls inside the CI -> can't reject "no difference"


def test_welch_t_test_handles_zero_variance_metrics():
    # peak memory / shuffle I/O are identical across every run in practice
    pyspark_vals = [96.0] * 10
    scala_vals = [96.0] * 10

    result = pe.welch_t_test(pyspark_vals, scala_vals)

    assert result["p_value"] is None
    assert result["t_stat"] is None
    assert result["mean_diff"] == 0.0
    assert "zero variance" in result["note"]


if __name__ == "__main__":
    test_parse_log_aggregates_correctly()
    test_peak_memory_is_max_not_sum_across_tasks()
    test_welch_t_test_detects_a_real_separation()
    test_welch_t_test_does_not_flag_overlapping_noise()
    test_welch_t_test_handles_zero_variance_metrics()
    print("All tests passed.")
