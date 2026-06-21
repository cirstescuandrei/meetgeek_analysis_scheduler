"""Shared benchmark metric definitions used by BOTH stacks.

E2E_LATENCY_BUCKETS_S - bucket boundaries (seconds) for the per-workflow end-to-end
latency histogram. Both stacks MUST use the same boundaries or their p50/p95/p99 are not
comparable: a histogram percentile is interpolated *within* a bucket, so a coarse bucket
inflates the tail. Temporal's SDK default jumps 600s -> 1800s (one 1200s-wide bucket);
our workflows cluster just above 600s, so the default put p95/p99 absurdly high (above the
batch wall-clock). These fine buckets (~60-120s resolution through the 600-900s range)
resolve the real tail.

  - Celery: passed straight to the prometheus_client Histogram (seconds).
  - Temporal: passed to PrometheusConfig.histogram_bucket_overrides for
    "workflow_endtoend_latency", scaled to ms (the SDK emits durations in ms by default).
"""

E2E_LATENCY_BUCKETS_S = (
    15, 30, 45, 60, 90, 120, 150, 180, 210, 240, 270, 300,
    360, 420, 480, 540, 600, 660, 720, 780, 840, 900,
    1020, 1140, 1260, 1500, 1800, 2400, 3000, 3600,
)
