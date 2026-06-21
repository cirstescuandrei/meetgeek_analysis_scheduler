"""End-to-end workflow latency metric for the Celery stack.

celery-exporter only exposes per-*task* runtime; it has no notion of a chain/workflow.
To compare like-for-like with Temporal's native `temporal_workflow_endtoend_latency`,
the final task observes the whole-workflow latency (client submit -> final task done)
into this histogram, and the worker process exposes it on :9000 - exactly how the
temporal workers expose their SDK metrics - so the benchmark harness can read p50/p95/p99
for both stacks the same way.
"""
import os

from celery.signals import worker_ready
from prometheus_client import Histogram, start_http_server

from meetgeek.metrics import E2E_LATENCY_BUCKETS_S

# Seconds (Temporal's histogram is in ms; the harness handles the unit per-stack). Bucket
# boundaries are SHARED with the Temporal stack (meetgeek.metrics) so the two stacks'
# p50/p95/p99 are computed over identical buckets and are actually comparable.
WORKFLOW_E2E = Histogram(
    "celery_workflow_endtoend_latency_seconds",
    "End-to-end latency of a meeting-analysis workflow: client submit to final task done.",
    buckets=(*E2E_LATENCY_BUCKETS_S, float("inf")),
)


@worker_ready.connect
def _start_metrics_server(**_):
    """Expose the histogram on METRICS_PORT (9000) once the worker is up. Runs on every
    celery worker; only the analysis worker (which runs the final task) records anything,
    and only celery-worker:9000 is scraped - the transcriber's endpoint stays empty."""
    try:
        start_http_server(int(os.getenv("METRICS_PORT", "9000")))
    except OSError:
        pass  # already bound / port taken - don't crash the worker over metrics
