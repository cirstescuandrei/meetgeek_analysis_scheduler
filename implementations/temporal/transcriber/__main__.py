import asyncio
import os
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
from implementations.temporal.shared import TRANSCRIBER_TASK_QUEUE

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
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_ACTIVITIES) as executor:
        worker = Worker(
            client,
            task_queue=TRANSCRIBER_TASK_QUEUE,
            activities=[transcript, speaker_diarization, language, silence],
            activity_executor=executor,
            max_concurrent_activities=MAX_CONCURRENT_ACTIVITIES,
        )
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
