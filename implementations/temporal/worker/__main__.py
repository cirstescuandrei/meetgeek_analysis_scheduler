import asyncio
import os
import signal
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

from implementations.temporal.activities import (
    unknown_speaker_inference,
    keyword_highlights,
    kpis,
    kpis_summary,
    meeting_workflows,
    summary,
    template,
    topics_with_highlights,
    vector_store,
    async_unknown_speaker_inference,
    async_keyword_highlights,
    async_kpis,
    async_kpis_summary,
    async_meeting_workflows,
    async_summary,
    async_template,
    async_topics_with_highlights,
    async_vector_store,
)
from implementations.temporal.shared import (
    GRACEFUL_SHUTDOWN,
    HEARTBEAT_INTERVAL,
    TASK_QUEUE,
)
from implementations.temporal.workflows import (
    AsyncMeetingAnalysisWorkflow,
    MeetingAnalysisWorkflow,
)
from meetgeek.metrics import E2E_LATENCY_BUCKETS_S

MAX_CONCURRENT_ACTIVITIES = 20


async def main() -> None:
    runtime = Runtime(
        telemetry=TelemetryConfig(
            metrics=PrometheusConfig(
                bind_address=os.getenv("METRICS_ADDRESS", "0.0.0.0:9000"),
                # The SDK's default endtoend-latency buckets are too coarse at the top
                # (600s -> 1800s in one bucket), so histogram-quantile interpolation
                # inflates p95/p99 above the real tail. Override with the shared fine
                # buckets (meetgeek.metrics), scaled to ms (durations are emitted in ms),
                # so Temporal and Celery percentiles are computed over identical buckets.
                histogram_bucket_overrides={
                    "workflow_endtoend_latency": [
                        b * 1000 for b in E2E_LATENCY_BUCKETS_S
                    ],
                },
            )
        )
    )
    client = await Client.connect(
        os.getenv("TEMPORAL_ADDRESS", "localhost:7233"), runtime=runtime
    )
    interrupt = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, interrupt.set)
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_ACTIVITIES) as executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[MeetingAnalysisWorkflow, AsyncMeetingAnalysisWorkflow],
            activities=[
                vector_store,
                unknown_speaker_inference,
                template,
                summary,
                topics_with_highlights,
                keyword_highlights,
                kpis,
                kpis_summary,
                meeting_workflows,
                async_vector_store,
                async_unknown_speaker_inference,
                async_template,
                async_summary,
                async_topics_with_highlights,
                async_keyword_highlights,
                async_kpis,
                async_kpis_summary,
                async_meeting_workflows,
            ],
            activity_executor=executor,
            max_concurrent_activities=MAX_CONCURRENT_ACTIVITIES,
            # Flush heartbeats to the server every ~INTERVAL (not the default ~24s),
            # giving a wide margin under the 30s heartbeat_timeout even for long activities.
            max_heartbeat_throttle_interval=HEARTBEAT_INTERVAL,
            default_heartbeat_throttle_interval=HEARTBEAT_INTERVAL,
            # On SIGTERM (scale-down) drain in-flight activities instead of orphaning them.
            graceful_shutdown_timeout=GRACEFUL_SHUTDOWN,
        )
        async with worker:
            await interrupt.wait()


if __name__ == "__main__":
    asyncio.run(main())
