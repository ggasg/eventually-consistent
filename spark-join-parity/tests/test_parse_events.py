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


if __name__ == "__main__":
    test_parse_log_aggregates_correctly()
    test_peak_memory_is_max_not_sum_across_tasks()
    print("All tests passed.")
