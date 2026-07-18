import json
import sys
import statistics as st
from collections import defaultdict

def parse_log(path):
    job_group = {}
    job_times = defaultdict(list)
    stage_to_job = {}
    tasks_by_stage = defaultdict(list)

    with open(path) as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        if not line:
            continue
        ev = json.loads(line)
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
    return results


def summarize(label, path):
    results = parse_log(path)
    runs = {k: v for k, v in results.items() if k.startswith("run_")}
    print(f"\n=== {label} ===")
    for tag in sorted(runs, key=lambda x: int(x.split("_")[1])):
        r = runs[tag]
        print(f"{tag}: dur={r['duration_ms']}ms gc={r['jvm_gc_time_ms']}ms "
              f"cpu={r['executor_cpu_time_ms']:.0f}ms peakMemMax={r['peak_execution_memory_max_mb']:.1f}MB "
              f"shuffle={r['shuffle_total_mb']:.1f}MB tasks={r['n_tasks']} "
              f"memSpill={r['memory_spill_mb']:.1f}MB")

    metrics = ["duration_ms", "jvm_gc_time_ms", "executor_cpu_time_ms",
               "peak_execution_memory_max_mb", "shuffle_total_mb", "memory_spill_mb"]
    avg = {}
    for m in metrics:
        vals = [runs[t][m] for t in runs if runs[t][m] is not None]
        avg[m] = st.mean(vals) if vals else None
    print(f"AVERAGE ({len(runs)} runs): " + ", ".join(f"{m}={avg[m]:.2f}" for m in metrics))
    return avg


if __name__ == "__main__":
    import glob
    pyspark_file = glob.glob("/tmp/bench/events/pyspark/*")[0]
    scala_file = glob.glob("/tmp/bench/events/scala/*")[0]

    avg_py = summarize("PySpark DataFrame join", pyspark_file)
    avg_sc = summarize("Scala Dataset joinWith", scala_file)

    print("\n=== DELTA (Scala - PySpark) ===")
    for m in avg_py:
        d = avg_sc[m] - avg_py[m]
        pct = (d / avg_py[m] * 100) if avg_py[m] else float("nan")
        print(f"{m}: scala={avg_sc[m]:.2f} pyspark={avg_py[m]:.2f} delta={d:.2f} ({pct:+.1f}%)")

    with open("/tmp/bench/results/summary.json", "w") as f:
        json.dump({"pyspark": avg_py, "scala": avg_sc}, f, indent=2)
