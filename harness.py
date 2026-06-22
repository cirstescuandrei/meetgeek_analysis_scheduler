#!/usr/bin/env python3
"""Benchmark harness: run a defined workload against one stack and record results.

Measures both stacks identically via Prometheus + wall-clock (no client changes):
  - wall-clock (submit -> all completed), throughput, completed/failed
  - peak worker/transcriber replicas, time-to-first-scale
  - (temporal only) native end-to-end workflow latency p50/p95/p99

Latency note: both stacks now expose a per-workflow end-to-end latency histogram -
Temporal natively (`temporal_workflow_endtoend_latency`, in ms), Celery via a custom
metric the final task records (`celery_workflow_endtoend_latency_seconds`, see
implementations/celery/metrics.py). The harness reads both the same way (bucket delta)
so e2e p50/p95/p99 are reported for both.

Usage:
  ./harness.py --target temporal --count 100 --size-mb 50
  ./harness.py --target celery   --count 100 --size-mb 50
  ./harness.py --target temporal --count 100 --size-mb 50 --mode async
"""
import argparse
import csv
import json
import os
import random
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
NS = "meetgeek"
RESULTS = os.path.join(HERE, "local", "benchmarks", "results.csv")
SIZE_DIST_CSV = os.path.join(HERE, "local", "parsed_output.csv")
SIZE_CAP_MB = 200.0  # match the SDK's modelled/valid range

DEPLOYMENTS = {
    "temporal": ("temporal-worker", "temporal-transcriber"),
    "celery": ("celery-worker", "celery-transcriber"),
}


def load_empirical_sizes(path):
    """Real meeting sizes (MB) from parsed_output.csv JOB_CONTEXT rows, cleaned to
    (0, SIZE_CAP_MB] - the range the duration model is calibrated for."""
    sizes = []
    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) >= 4 and row[0] == "JOB_CONTEXT":
                try:
                    mb = float(row[3].split(":", 1)[1])
                    if 0 < mb <= SIZE_CAP_MB:
                        sizes.append(mb)
                except (ValueError, IndexError):
                    continue
    if not sizes:
        raise SystemExit(f"no usable sizes in {path}")
    return sizes


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


def ebuckets(target, wf):
    """Snapshot the e2e-latency histogram as {le_seconds: cumulative_count}.

    Temporal: `temporal_workflow_endtoend_latency_bucket` with `le` in ms (/1000).
    Celery:   `celery_workflow_endtoend_latency_seconds_bucket` with `le` already in s.
    """
    if target == "temporal":
        metric = f'temporal_workflow_endtoend_latency_bucket{{workflow_type="{wf}"}}'
        to_s = 1000.0
    else:
        metric = "celery_workflow_endtoend_latency_seconds_bucket"
        to_s = 1.0
    q = urllib.parse.quote(f"sum({metric}) by (le)")
    out = sh(f"kubectl exec -n {NS} deploy/prometheus -- wget -qO- "
             f"'http://localhost:9090/api/v1/query?query={q}' 2>/dev/null")
    snap = {}
    try:
        for r in json.loads(out)["data"]["result"]:
            le = float(r["metric"]["le"])  # "+Inf" -> inf
            snap[le / to_s if le != float("inf") else float("inf")] = float(r["value"][1])
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
    """Submit one workflow. Returns the id used to track completion (Celery: the chain's
    result id; Temporal: the workflow id), or None on error."""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = json.loads(r.read().decode())
            return body.get("task_id") or body.get("workflow_id")
    except Exception:
        return None


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


def temporal_count(wf, status):
    """Authoritative count of workflows in a terminal status from the Temporal server's
    visibility store. Robust to worker autoscaling: the per-worker SDK counters in
    Prometheus undercount because workers are scraped through a load-balanced Service and
    scaled-down pods take their local counts with them."""
    out = sh(f"kubectl exec -n {NS} deploy/temporal -c temporal -- "
             f"temporal workflow count "
             f"--address temporal.{NS}.svc.cluster.local:7233 --namespace default "
             f"--query \"WorkflowType='{wf}' AND ExecutionStatus='{status}'\" 2>/dev/null")
    m = re.search(r"Total:\s*(\d+)", out)
    return float(m.group(1)) if m else 0.0


def completed_count(target, wf):
    """Number of completed workflows. Temporal: server visibility. Celery: the central
    celery-exporter counter, which already aggregates across all workers."""
    if target == "temporal":
        return temporal_count(wf, "Completed")
    return promql(completed_query(target, None))


def failed_count(target, wf):
    if target == "temporal":
        return temporal_count(wf, "Failed")
    return promql(failed_query(target, None))


def celery_results(ids):
    """Count terminal Celery chains (succeeded, failed) by their final-task ids in the
    durable Redis result backend. Robust to worker autoscaling, unlike the per-hostname
    celery-exporter counters whose sum drops when scaled-down workers' series disappear."""
    if not ids:
        return 0, 0
    keys = " ".join(f"celery-task-meta-{i}" for i in ids)
    out = sh(f"kubectl exec -n {NS} deploy/redis -- redis-cli mget {keys} 2>/dev/null")
    ok = bad = 0
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            status = json.loads(line).get("status")
        except Exception:
            continue
        if status == "SUCCESS":
            ok += 1
        elif status in ("FAILURE", "REVOKED"):
            bad += 1
    return ok, bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True, choices=["temporal", "celery"])
    ap.add_argument("--count", type=int, required=True)
    ap.add_argument("--size-mb", type=float, default=50.0,
                    help="fixed meeting size (ignored when --size-dist is given)")
    ap.add_argument("--size-dist", nargs="?", const=SIZE_DIST_CSV, default=None,
                    metavar="CSV",
                    help="sample each meeting's size from the empirical distribution "
                         "(default: local/parsed_output.csv); overrides --size-mb")
    ap.add_argument("--mode", choices=["sync", "async"], default="async", help="temporal only")
    ap.add_argument("--seed", type=int, default=None,
                    help="seed size sampling so --size-dist runs are reproducible "
                         "(use the same seed for temporal & celery to compare on an "
                         "identical mixed workload)")
    ap.add_argument("--fail-rate", type=float, default=0.0,
                    help="per-activity failure probability (failure_rate in the payload)")
    ap.add_argument("--concurrency", type=int, default=50, help="parallel submit workers")
    # Batch-completion wait. Activity/task timeouts are 30 min/stage and real durations
    # are long, so a full batch can run well beyond that; default 1 h, raise for big runs.
    ap.add_argument("--timeout", type=int, default=3600, help="max seconds to wait for completion")
    args = ap.parse_args()

    target, count = args.target, args.count
    if args.seed is not None:
        random.seed(args.seed)
    if args.size_dist:
        pool = load_empirical_sizes(args.size_dist)
        sizes_mb = [random.choice(pool) for _ in range(count)]
        size = round(sum(sizes_mb) / count, 1)        # mean, recorded as the run's size
        size_label = f"dist(mean {size}MB, {min(sizes_mb):.0f}-{max(sizes_mb):.0f})"
    else:
        sizes_mb = [args.size_mb] * count
        size = args.size_mb
        size_label = f"{size}MB"
    worker_dep, transcriber_dep = DEPLOYMENTS[target]
    url = client_url(target)

    print(f">> {target} | count={count} size={size_label} mode={args.mode} "
          f"failure_rate={args.fail_rate} | url={url}")

    wf = "AsyncMeetingAnalysisWorkflow" if args.mode == "async" else "MeetingAnalysisWorkflow"
    fr_active = bool(args.fail_rate) and float(args.fail_rate) > 0
    base_completed = base_failed = 0
    if target == "temporal":
        # Pre-flight: wait until the visibility count stops moving, so stragglers from prior
        # runs don't contaminate this batch's baseline / completion detection. Celery tracks
        # this run's own chain ids, so it needs no baseline.
        prev, stable = completed_count(target, wf), 0
        while stable < 3:
            time.sleep(4)
            cur = completed_count(target, wf)
            stable = stable + 1 if cur == prev else 0
            if cur != prev:
                print(f"   waiting for cluster to quiesce (completed={int(cur)})")
            prev = cur
        base_completed = prev
        base_failed = failed_count(target, wf)
    base_buckets = ebuckets(target, wf)

    payloads = []
    for i in range(count):
        payloads.append({
            "title": f"bench-{i}", "size": int(sizes_mb[i] * 1024 * 1024),
            "failure_rate": args.fail_rate,
            **({"mode": args.mode} if target == "temporal" else {}),
        })

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        ids = [r for r in ex.map(lambda p: submit(url, p), payloads) if r]
    accepted = len(ids)
    print(f">> submitted {accepted}/{count} in {time.time()-t0:.1f}s; waiting for completion...")

    # Wait for every meeting to reach a terminal state, then record (completed, failed) from
    # durable sources that survive worker autoscaling.
    #   Temporal -> server visibility deltas.
    #   Celery   -> successes from the Redis result backend (this run's chain ids). A chain
    #               that aborts mid-way leaves no result on its final id, so failures cannot
    #               be read per-chain; instead the run is terminal once the broker queues
    #               drain (no ready or in-flight tasks), and the remainder counts as failed.
    def broker_idle():
        out = sh(f"kubectl exec -n {NS} deploy/rabbitmq -- "
                 f"rabbitmqctl list_queues name messages 2>/dev/null")
        pending = 0
        for line in out.splitlines():
            p = line.split()
            if len(p) == 2 and p[0] in ("meeting-transcription", "meeting-analysis"):
                pending += int(p[1]) if p[1].isdigit() else 0
        return pending == 0

    t_end = None
    completed = failed = idle = 0
    while time.time() - t0 < args.timeout:
        if target == "temporal":
            completed = completed_count(target, wf) - base_completed
            failed = (failed_count(target, wf) - base_failed) if fr_active else 0
            terminal = completed + failed >= count
        else:
            completed, _ = celery_results(ids)
            if completed >= count:
                failed, terminal = 0, True
            else:
                idle = idle + 1 if broker_idle() else 0
                terminal = idle >= 4   # ~12s with no pending or in-flight tasks
                failed = count - completed if terminal else 0
        done = completed + failed
        sys.stdout.write(f"\r   terminal {int(done)}/{count}   ({time.time()-t0:.0f}s)")
        sys.stdout.flush()
        if terminal:
            t_end = time.time()
            break
        time.sleep(3)
    print()
    if t_end is None:
        t_end = time.time()
        print(f"!! TIMEOUT after {args.timeout}s - recording partial results")

    wall = t_end - t0
    window = int(wall) + 15

    peak_worker = promql(
        f'max_over_time(kube_deployment_status_replicas_available'
        f'{{namespace="{NS}",deployment="{worker_dep}"}}[{window}s])')
    peak_transcriber = promql(
        f'max_over_time(kube_deployment_status_replicas_available'
        f'{{namespace="{NS}",deployment="{transcriber_dep}"}}[{window}s])')

    # time-to-first-scale: first sample where worker replicas > 1, relative to t0
    tts = scale_time(worker_dep, t0, t_end)

    end_buckets = ebuckets(target, wf)
    p50 = hist_quantile(base_buckets, end_buckets, 0.50)
    p95 = hist_quantile(base_buckets, end_buckets, 0.95)
    p99 = hist_quantile(base_buckets, end_buckets, 0.99)
    p50, p95, p99 = (x if x is not None else "" for x in (p50, p95, p99))

    row = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "target": target, "mode": args.mode if target == "temporal" else "",
        "count": count, "size_mb": size, "failure_rate": args.fail_rate,
        "seed": args.seed if args.seed is not None else "",
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
