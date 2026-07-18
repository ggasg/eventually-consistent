import json
import math
import statistics as st
import sys
from collections import defaultdict

from scipy import stats as sstats


def parse_log(path):
    """Parses a Spark JSON event log. Tolerates a truncated final line:
    an event log file still open when the driver process is killed ends
    mid-write, and that partial line isn't a parsing bug to fix, it's
    what an interrupted log looks like. The run it belongs to gets
    dropped downstream in summarize() for lacking a matching job-end
    event, not silently kept with a fabricated value."""
    job_group = {}
    job_times = defaultdict(list)
    stage_to_job = {}
    tasks_by_stage = defaultdict(list)

    with open(path) as f:
        lines = f.readlines()

    skipped = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            skipped += 1
            continue
        etype = ev.get("Event")

        if etype == "SparkListenerJobStart":
            job_id = ev["Job ID"]
            props = ev.get("Properties", {}) or {}
            group = props.get("spark.jobGroup.id", "")
            job_group[job_id] = group
            sub_time = ev.get("Submission Time")
            for sinfo in ev.get("Stage Infos", []):
                stage_to_job[sinfo["Stage ID"]] = job_id
            job_times[(job_id, group)].append(["start", sub_time])

        elif etype == "SparkListenerJobEnd":
            job_id = ev["Job ID"]
            comp_time = ev.get("Completion Time")
            group = job_group.get(job_id, "")
            job_times[(job_id, group)].append(["end", comp_time])

        elif etype == "SparkListenerTaskEnd":
            stage_id = ev.get("Stage ID")
            tm = ev.get("Task Metrics")
            if tm is None:
                continue
            tasks_by_stage[stage_id].append(tm)

    group_job_span = defaultdict(lambda: [None, None])
    for (job_id, group), events_list in job_times.items():
        starts = [t for typ, t in events_list if typ == "start" and t is not None]
        ends = [t for typ, t in events_list if typ == "end" and t is not None]
        if starts:
            s = min(starts)
            cur = group_job_span[group][0]
            group_job_span[group][0] = s if cur is None else min(cur, s)
        if ends:
            e = max(ends)
            cur = group_job_span[group][1]
            group_job_span[group][1] = e if cur is None else max(cur, e)

    group_stages = defaultdict(set)
    for stage_id, job_id in stage_to_job.items():
        group = job_group.get(job_id, "")
        group_stages[group].add(stage_id)

    results = {}
    for group, stage_ids in group_stages.items():
        if not group:
            continue
        gc_time = 0
        exec_run_time = 0
        exec_cpu_time_ns = 0
        peak_mem_values = []
        shuffle_read_bytes = 0
        shuffle_write_bytes = 0
        n_tasks = 0
        mem_spill = 0
        disk_spill = 0

        for sid in stage_ids:
            for tm in tasks_by_stage.get(sid, []):
                n_tasks += 1
                gc_time += tm.get("JVM GC Time", 0) or 0
                exec_run_time += tm.get("Executor Run Time", 0) or 0
                exec_cpu_time_ns += tm.get("Executor CPU Time", 0) or 0
                mem_spill += tm.get("Memory Bytes Spilled", 0) or 0
                disk_spill += tm.get("Disk Bytes Spilled", 0) or 0
                peak = tm.get("Peak Execution Memory", 0) or 0
                peak_mem_values.append(peak)

                srm = tm.get("Shuffle Read Metrics")
                if srm:
                    shuffle_read_bytes += (srm.get("Local Bytes Read", 0) or 0)
                    shuffle_read_bytes += (srm.get("Remote Bytes Read", 0) or 0)

                swm = tm.get("Shuffle Write Metrics")
                if swm:
                    shuffle_write_bytes += (swm.get("Shuffle Bytes Written", 0) or 0)

        span = group_job_span.get(group, [None, None])
        duration_ms = None
        if span[0] is not None and span[1] is not None:
            duration_ms = span[1] - span[0]

        results[group] = {
            "n_tasks": n_tasks,
            "duration_ms": duration_ms,
            "jvm_gc_time_ms": gc_time,
            "executor_run_time_ms": exec_run_time,
            "executor_cpu_time_ms": exec_cpu_time_ns / 1e6,
            "peak_execution_memory_max_mb": (max(peak_mem_values) / (1024 * 1024)) if peak_mem_values else 0,
            "peak_execution_memory_sum_mb": (sum(peak_mem_values) / (1024 * 1024)) if peak_mem_values else 0,
            "shuffle_read_mb": shuffle_read_bytes / (1024 * 1024),
            "shuffle_write_mb": shuffle_write_bytes / (1024 * 1024),
            "shuffle_total_mb": (shuffle_read_bytes + shuffle_write_bytes) / (1024 * 1024),
            "memory_spill_mb": mem_spill / (1024 * 1024),
            "disk_spill_mb": disk_spill / (1024 * 1024),
        }
    if skipped:
        print(f"[parse_log] skipped {skipped} malformed line(s) in {path}", file=sys.stderr)
    return results


METRICS = [
    "duration_ms",
    "jvm_gc_time_ms",
    "executor_cpu_time_ms",
    "peak_execution_memory_max_mb",
    "shuffle_total_mb",
    "memory_spill_mb",
]


def summarize(label, path):
    """Returns (stat_summary, raw) where raw[metric] is the list of
    per-run values, in run order, so a reader can rerun their own test
    against the same numbers instead of trusting our p-value."""
    results = parse_log(path)
    runs = {k: v for k, v in results.items() if k.startswith("run_")}
    ordered_tags = sorted(runs, key=lambda x: int(x.split("_")[1]))

    # A run missing its job-end event (driver killed mid-run, log still
    # ".inprogress") has partial task metrics across the board, not just
    # a missing duration, so the whole run is dropped rather than mixing
    # a truncated task count into the other metrics.
    incomplete = [t for t in ordered_tags if runs[t]["duration_ms"] is None]
    if incomplete:
        print(f"[{label}] dropping incomplete run(s), no matching job-end event: {incomplete}",
              file=sys.stderr)
        ordered_tags = [t for t in ordered_tags if t not in incomplete]

    print(f"\n=== {label} ===")
    for tag in ordered_tags:
        r = runs[tag]
        print(f"{tag}: dur={r['duration_ms']}ms gc={r['jvm_gc_time_ms']}ms "
              f"cpu={r['executor_cpu_time_ms']:.0f}ms peakMemMax={r['peak_execution_memory_max_mb']:.1f}MB "
              f"shuffle={r['shuffle_total_mb']:.1f}MB tasks={r['n_tasks']} "
              f"memSpill={r['memory_spill_mb']:.1f}MB")

    raw = {}
    stat_summary = {}
    for m in METRICS:
        vals = [runs[t][m] for t in ordered_tags if runs[t][m] is not None]
        raw[m] = vals
        stat_summary[m] = {
            "mean": st.mean(vals) if vals else None,
            "stdev": st.stdev(vals) if len(vals) > 1 else 0.0,
            "min": min(vals) if vals else None,
            "max": max(vals) if vals else None,
        }
    print(f"AVERAGE ({len(ordered_tags)} runs): " +
          ", ".join(f"{m}={stat_summary[m]['mean']:.2f}" for m in METRICS))
    return stat_summary, raw


def welch_t_test(pyspark_vals, scala_vals):
    """Welch's two-sample t-test (unequal variances assumed) plus a 95%
    CI on the mean difference, scala - pyspark, matching the Delta
    column's sign convention. Falls back to a plain equality check when
    a metric has zero variance in both samples (peak memory and shuffle
    I/O are identical across every run, so a t-test is undefined there,
    not merely non-significant)."""
    a, b = list(pyspark_vals), list(scala_vals)
    n_a, n_b = len(a), len(b)
    var_a = st.variance(a) if n_a > 1 else 0.0
    var_b = st.variance(b) if n_b > 1 else 0.0
    mean_diff = st.mean(b) - st.mean(a)

    if var_a == 0.0 and var_b == 0.0:
        return {
            "mean_diff": mean_diff,
            "t_stat": None,
            "p_value": None,
            "df": None,
            "ci_95": [mean_diff, mean_diff],
            "note": "zero variance in both samples; identical every run, not just on average",
        }

    t_stat, p_value = sstats.ttest_ind(b, a, equal_var=False)
    se = math.sqrt(var_a / n_a + var_b / n_b)
    df = (var_a / n_a + var_b / n_b) ** 2 / (
        (var_a / n_a) ** 2 / (n_a - 1) + (var_b / n_b) ** 2 / (n_b - 1)
    )
    t_crit = sstats.t.ppf(0.975, df)
    ci_95 = [mean_diff - t_crit * se, mean_diff + t_crit * se]
    return {
        "mean_diff": float(mean_diff),
        "t_stat": float(t_stat),
        "p_value": float(p_value),
        "df": float(df),
        "ci_95": [float(ci_95[0]), float(ci_95[1])],
    }


if __name__ == "__main__":
    import argparse
    import glob

    # --events-dir / --out-prefix exist so a single benchmark session (one
    # events dir, one pair of event log files) can be parsed into its own
    # named results file without clobbering another session's output.
    # That's what let three separate sessions in this repo's history
    # (see results/session_*.json) get compared side by side instead of
    # each overwriting the last.
    argp = argparse.ArgumentParser()
    argp.add_argument("--events-dir", default="/tmp/bench/events",
                       help="directory containing pyspark/ and scala/ event log subdirs")
    argp.add_argument("--out-prefix", default="summary",
                       help="results/<prefix>.json and results/<prefix>_raw.json")
    args = argp.parse_args()

    pyspark_file = glob.glob(f"{args.events_dir}/pyspark/*")[0]
    scala_file = glob.glob(f"{args.events_dir}/scala/*")[0]

    stat_py, raw_py = summarize("PySpark DataFrame join", pyspark_file)
    stat_sc, raw_sc = summarize("Scala Dataset joinWith", scala_file)

    print("\n=== WELCH'S T-TEST (Scala - PySpark), 95% CI on the mean difference ===")
    tests = {}
    for m in METRICS:
        result = welch_t_test(raw_py[m], raw_sc[m])
        tests[m] = result
        if result.get("p_value") is None:
            print(f"{m}: {result['note']} (mean_diff={result['mean_diff']:.4f})")
        else:
            lo, hi = result["ci_95"]
            print(f"{m}: mean_diff={result['mean_diff']:.4f} t={result['t_stat']:.3f} "
                  f"df={result['df']:.1f} p={result['p_value']:.4f} 95%CI=({lo:.4f}, {hi:.4f})")

    with open(f"/tmp/bench/results/{args.out_prefix}.json", "w") as f:
        json.dump({
            "pyspark": stat_py,
            "scala": stat_sc,
            "delta_scala_minus_pyspark": tests,
            "n_runs_pyspark": len(next(iter(raw_py.values()))),
            "n_runs_scala": len(next(iter(raw_sc.values()))),
        }, f, indent=2)

    with open(f"/tmp/bench/results/{args.out_prefix}_raw.json", "w") as f:
        json.dump({"pyspark": raw_py, "scala": raw_sc}, f, indent=2)
