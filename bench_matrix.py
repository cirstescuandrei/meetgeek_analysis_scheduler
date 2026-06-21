#!/usr/bin/env python3
"""Run harness.py across a grid of (size, count, failure_rate) against BOTH stacks,
with repetitions, and print a side-by-side table. Each harness run appends one row to
local/benchmarks/results.csv; this wrapper reads back the row it just produced.

Runs are sequential (the cluster can't fairly run two benchmarks at once). Before each
run the wrapper waits for that stack's worker+transcriber to scale back to 1, so every
run starts from a clean autoscaling baseline.

Usage:
  ./bench_matrix.py --sizes 5,25,100 --counts 20,50 --reps 3
  ./bench_matrix.py --counts 50,100 --reps 3 --size-dist          # realistic mixed workload
  ./bench_matrix.py --counts 50 --fail-rates 0,0.1,0.2,0.3 --reps 3
  ./bench_matrix.py --sizes 5 --counts 20 --mode async
"""
import argparse
import csv
import os
import subprocess
import time

HERE = os.path.dirname(os.path.abspath(__file__))
HARNESS = os.path.join(HERE, "harness.py")
RESULTS = os.path.join(HERE, "local", "benchmarks", "results.csv")
NS = "meetgeek"
TARGETS = ("temporal", "celery")
DEPLOYMENTS = {
    "temporal": ("temporal-worker", "temporal-transcriber"),
    "celery": ("celery-worker", "celery-transcriber"),
}
COLS = [
    ("wall_clock_s", "wall_s"),
    ("throughput_per_min", "thru/min"),
    ("completed", "done"),
    ("failed", "failed"),
    ("peak_worker_replicas", "pk_wrk"),
    ("peak_transcriber_replicas", "pk_trn"),
    ("time_to_scale_s", "t_scale"),
    ("e2e_p95_s", "p95_s"),
]


def available(dep):
    out = subprocess.run(
        ["kubectl", "get", "deploy", dep, "-n", NS,
         "-o", "jsonpath={.status.availableReplicas}"],
        capture_output=True, text=True).stdout.strip()
    return int(out) if out.isdigit() else 0


def wait_baseline(target, timeout):
    """Wait until the target stack's worker + transcriber are back to <=1 replica."""
    if timeout <= 0:
        return
    deps = DEPLOYMENTS[target]
    t0 = time.time()
    while time.time() - t0 < timeout:
        reps = [available(d) for d in deps]
        if all(r <= 1 for r in reps):
            return
        print(f"   cooldown: waiting for {target} to scale down {deps}={reps}")
        time.sleep(10)


def last_row():
    if not os.path.exists(RESULTS):
        return None
    rows = list(csv.DictReader(open(RESULTS)))
    return rows[-1] if rows else None


def run(target, size, count, fail_rate, mode, timeout, seed, cooldown):
    wait_baseline(target, cooldown)
    cmd = [HARNESS, "--target", target, "--count", str(count),
           "--fail-rate", str(fail_rate), "--timeout", str(timeout)]
    if size is None:                       # --size-dist
        cmd += ["--size-dist"]
        if seed is not None:
            cmd += ["--seed", str(seed)]
    else:
        cmd += ["--size-mb", str(size)]
    if target == "temporal":
        cmd += ["--mode", mode]
    print(f"\n>>> {' '.join(cmd)}")
    subprocess.run(cmd, check=False)
    return last_row()


def print_table(results):
    cols = [("count", "count"), ("size", "size"), ("fr", "fr"), ("rep", "rep"),
            ("target", "target")] + COLS
    head = " ".join(f"{h:>9}" for _, h in cols)
    print("\n===================== BENCHMARK MATRIX =====================")
    print(head)
    print("-" * len(head))
    last_key = None
    for count, size, fr, rep, target, row in results:
        if last_key is not None and (count, size, fr) != last_key:
            print()
        last_key = (count, size, fr)
        r = row or {}
        base = {"count": count, "size": "dist" if size is None else size,
                "fr": fr, "rep": rep, "target": target}
        vals = " ".join(f"{str(base.get(k, r.get(k, '?'))):>9}" for k, _ in cols)
        print(vals)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="25", help="comma-separated meeting sizes (MB)")
    ap.add_argument("--counts", default="20", help="comma-separated workflow counts")
    ap.add_argument("--fail-rates", default="0", help="comma-separated per-activity failure probs")
    ap.add_argument("--reps", type=int, default=1, help="repetitions per cell")
    ap.add_argument("--size-dist", action="store_true",
                    help="sample sizes from the empirical distribution (ignores --sizes); "
                         "both stacks in a cell+rep use the same seed for an identical workload")
    ap.add_argument("--seed", type=int, default=0, help="base seed for --size-dist (per rep: seed+rep)")
    ap.add_argument("--mode", default="async", choices=["sync", "async"], help="temporal mode")
    ap.add_argument("--timeout", type=int, default=3600, help="per-run completion timeout (s)")
    ap.add_argument("--cooldown", type=int, default=240,
                    help="max seconds to wait for scale-down before each run (0 = skip)")
    args = ap.parse_args()

    sizes = [None] if args.size_dist else [float(x) for x in args.sizes.split(",")]
    counts = [int(x) for x in args.counts.split(",")]
    fail_rates = [float(x) for x in args.fail_rates.split(",")]

    results = []
    for count in counts:
        for size in sizes:
            for fr in fail_rates:
                for rep in range(args.reps):
                    seed = (args.seed + rep) if args.size_dist else None
                    for target in TARGETS:
                        row = run(target, size, count, fr, args.mode,
                                  args.timeout, seed, args.cooldown)
                        results.append((count, size, fr, rep, target, row))
    print_table(results)
    print(f"\nfull history: {RESULTS}")


if __name__ == "__main__":
    main()
