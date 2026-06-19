#!/usr/bin/env python3
"""Benchmark harness: run a defined workload against one stack and record results.

Measures both stacks identically via Prometheus + wall-clock (no client changes):
  - wall-clock (submit -> all completed), throughput, completed/failed
  - peak worker/transcriber replicas, time-to-first-scale
  - (temporal only) native end-to-end workflow latency p50/p95/p99

Latency note: Temporal exposes true per-workflow end-to-end latency; Celery's
exporter only exposes per-task runtime, so e2e percentiles are temporal-only and
reported as n/a for celery. Throughput + wall-clock are the fair cross-stack metrics.

Usage:
  ./harness.py --target temporal --count 100 --size-mb 50
  ./harness.py --target celery   --count 100 --size-mb 50
  ./harness.py --target temporal --count 100 --size-mb 50 --mode async
"""
import argparse
import csv
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

NS = "meetgeek"
RESULTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local", "benchmarks", "results.csv")

DEPLOYMENTS = {
    "temporal": ("temporal-worker", "temporal-transcriber"),
    "celery": ("celery-worker", "celery-transcriber"),
}


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()


def promql(query):
    """Instant query via the in-cluster prometheus pod. Returns float (summed) or 0."""
    q = urllib.parse.quote(query)
    out = sh(f"kubectl exec -n {NS} deploy/prometheus -- wget -qO- "
             f"'http://localhost:9090/api/v1/query?query={q}' 2>/dev/null")
    try:
        res = json.loads(out)["data"]["result"]
        return sum(float(r["value"][1]) for r in res) if res else 0.0
    except Exception:
        return 0.0


def ebuckets(wf):
    """Snapshot the Temporal e2e-latency histogram as {le_seconds: cumulative_count}."""
    q = urllib.parse.quote(
        f'sum(temporal_workflow_endtoend_latency_bucket{{workflow_type="{wf}"}}) by (le)')
    out = sh(f"kubectl exec -n {NS} deploy/prometheus -- wget -qO- "
             f"'http://localhost:9090/api/v1/query?query={q}' 2>/dev/null")
    snap = {}
    try:
        for r in json.loads(out)["data"]["result"]:
            le = float(r["metric"]["le"])  # ms; "+Inf" -> inf
            snap[le / 1000.0 if le != float("inf") else float("inf")] = float(r["value"][1])
    except Exception:
        pass
    return snap


def hist_quantile(base, end, q):
    """Quantile (seconds) over the per-batch delta of cumulative histogram buckets."""
    les = sorted(end)
    delta = [(le, end.get(le, 0) - base.get(le, 0)) for le in les]
    total = delta[-1][1] if delta else 0
    if total <= 0:
        return None
    rank = q * total
    prev_le, prev_c = 0.0, 0.0
    for le, c in delta:
        if c >= rank:
            if le == float("inf"):
                return prev_le
            if c == prev_c:
                return le
            return prev_le + (rank - prev_c) / (c - prev_c) * (le - prev_le)
        prev_le, prev_c = le, c
    return prev_le


def client_url(target):
    ip = sh("minikube ip")
    port = sh(f"kubectl get svc {target}-client -n {NS} "
              f"-o jsonpath='{{.spec.ports[0].nodePort}}'")
    return f"http://{ip}:{port}/analyze"


def submit(url, payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status == 200
    except Exception:
        return False


def completed_query(target, mode):
    if target == "temporal":
        wf = "AsyncMeetingAnalysisWorkflow" if mode == "async" else "MeetingAnalysisWorkflow"
        return f'sum(temporal_workflow_completed{{workflow_type="{wf}"}})'
    return 'sum(celery_task_succeeded_total{name="implementations.celery.tasks.meeting_workflows"})'


def failed_query(target, mode):
    if target == "temporal":
        wf = "AsyncMeetingAnalysisWorkflow" if mode == "async" else "MeetingAnalysisWorkflow"
        return f'sum(temporal_workflow_failed{{workflow_type="{wf}"}})'
    return 'sum(celery_task_failed_total{exception!=""})'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True, choices=["temporal", "celery"])
    ap.add_argument("--count", type=int, required=True)
    ap.add_argument("--size-mb", type=float, default=50.0)
    ap.add_argument("--mode", choices=["sync", "async"], default="sync", help="temporal only")
    ap.add_argument("--fail-rate", type=float, default=0.0, help="fraction submitted with should_fail")
    ap.add_argument("--concurrency", type=int, default=50, help="parallel submit workers")
    ap.add_argument("--timeout", type=int, default=900, help="max seconds to wait for completion")
    args = ap.parse_args()

    target, count, size = args.target, args.count, args.size_mb
    size_bytes = int(size * 1024 * 1024)
    n_fail = round(count * args.fail_rate)
    n_ok = count - n_fail
    worker_dep, transcriber_dep = DEPLOYMENTS[target]
    url = client_url(target)

    print(f">> {target} | count={count} size={size}MB mode={args.mode} "
          f"fail={n_fail} | url={url}")

    comp_q, fail_q = completed_query(target, args.mode), failed_query(target, args.mode)
    base_completed = promql(comp_q)
    base_failed = promql(fail_q)
    wf = "AsyncMeetingAnalysisWorkflow" if args.mode == "async" else "MeetingAnalysisWorkflow"
    base_buckets = ebuckets(wf) if target == "temporal" else {}

    payloads = []
    for i in range(count):
        payloads.append({
            "title": f"bench-{i}", "size": size_bytes,
            "should_fail": i < n_fail,
            **({"mode": args.mode} if target == "temporal" else {}),
        })

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        accepted = sum(ex.map(lambda p: submit(url, p), payloads))
    print(f">> submitted {accepted}/{count} in {time.time()-t0:.1f}s; waiting for completion...")

    target_completed = base_completed + n_ok
    t_end = None
    while time.time() - t0 < args.timeout:
        done = promql(comp_q) - base_completed
        sys.stdout.write(f"\r   completed {int(done)}/{n_ok}   ({time.time()-t0:.0f}s)")
        sys.stdout.flush()
        if promql(comp_q) >= target_completed:
            t_end = time.time()
            break
        time.sleep(3)
    print()
    if t_end is None:
        t_end = time.time()
        print(f"!! TIMEOUT after {args.timeout}s — recording partial results")

    wall = t_end - t0
    completed = promql(comp_q) - base_completed
    failed = promql(fail_q) - base_failed
    window = int(wall) + 15

    peak_worker = promql(
        f'max_over_time(kube_deployment_status_replicas_available'
        f'{{namespace="{NS}",deployment="{worker_dep}"}}[{window}s])')
    peak_transcriber = promql(
        f'max_over_time(kube_deployment_status_replicas_available'
        f'{{namespace="{NS}",deployment="{transcriber_dep}"}}[{window}s])')

    # time-to-first-scale: first sample where worker replicas > 1, relative to t0
    tts = scale_time(worker_dep, t0, t_end)

    if target == "temporal":
        end_buckets = ebuckets(wf)
        p50 = hist_quantile(base_buckets, end_buckets, 0.50)
        p95 = hist_quantile(base_buckets, end_buckets, 0.95)
        p99 = hist_quantile(base_buckets, end_buckets, 0.99)
        p50, p95, p99 = (x if x is not None else "" for x in (p50, p95, p99))
    else:
        p50 = p95 = p99 = ""

    row = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "target": target, "mode": args.mode if target == "temporal" else "",
        "count": count, "size_mb": size, "fail_submitted": n_fail,
        "wall_clock_s": round(wall, 1),
        "throughput_per_min": round(completed / wall * 60, 1) if wall else 0,
        "completed": int(completed), "failed": int(failed),
        "peak_worker_replicas": int(peak_worker),
        "peak_transcriber_replicas": int(peak_transcriber),
        "time_to_scale_s": tts,
        "e2e_p50_s": round(p50, 1) if p50 != "" else "",
        "e2e_p95_s": round(p95, 1) if p95 != "" else "",
        "e2e_p99_s": round(p99, 1) if p99 != "" else "",
    }
    write_row(row)
    print_row(row)


def scale_time(worker_dep, t0, t_end):
    q = urllib.parse.quote(
        f'kube_deployment_status_replicas_available'
        f'{{namespace="{NS}",deployment="{worker_dep}"}}')
    out = sh(f"kubectl exec -n {NS} deploy/prometheus -- wget -qO- "
             f"'http://localhost:9090/api/v1/query_range?query={q}"
             f"&start={t0}&end={t_end}&step=5' 2>/dev/null")
    try:
        vals = json.loads(out)["data"]["result"][0]["values"]
        for ts, v in vals:
            if float(v) > 1:
                return round(float(ts) - t0, 1)
    except Exception:
        pass
    return "none"


def write_row(row):
    os.makedirs(os.path.dirname(RESULTS), exist_ok=True)
    new = not os.path.exists(RESULTS)
    with open(RESULTS, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if new:
            w.writeheader()
        w.writerow(row)


def print_row(row):
    print("\n=== result ===")
    for k, v in row.items():
        print(f"  {k:<26} {v}")
    print(f"\nappended to {RESULTS}")


if __name__ == "__main__":
    main()
