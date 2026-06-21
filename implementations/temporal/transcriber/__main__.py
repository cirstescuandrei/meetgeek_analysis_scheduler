import asyncio
import os
import signal
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

from implementations.temporal.activities import (
    language,
    silence,
    speaker_diarization,
    transcript,
)
from implementations.temporal.shared import (
    GRACEFUL_SHUTDOWN,
    HEARTBEAT_INTERVAL,
    TRANSCRIBER_TASK_QUEUE,
)

MAX_CONCURRENT_ACTIVITIES = 4


async def main() -> None:
    runtime = Runtime(
        telemetry=TelemetryConfig(
            metrics=PrometheusConfig(
                bind_address=os.getenv("METRICS_ADDRESS", "0.0.0.0:9000")
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
            task_queue=TRANSCRIBER_TASK_QUEUE,
            activities=[transcript, speaker_diarization, language, silence],
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
