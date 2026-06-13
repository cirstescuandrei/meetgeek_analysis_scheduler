import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

from implementations.temporal.activities import (
    unknown_speaker_inference,
    keyword_highlights,
    kpis,
    kpis_summary,
    language,
    meeting_workflows,
    silence,
    speaker_diarization,
    summary,
    template,
    topics_with_highlights,
    transcript,
    vector_store,
    async_unknown_speaker_inference,
    async_keyword_highlights,
    async_kpis,
    async_kpis_summary,
    async_language,
    async_meeting_workflows,
    async_silence,
    async_speaker_diarization,
    async_summary,
    async_template,
    async_topics_with_highlights,
    async_transcript,
    async_vector_store,
)
from implementations.temporal.shared import TASK_QUEUE
from implementations.temporal.workflows import (
    AsyncMeetingAnalysisWorkflow,
    MeetingAnalysisWorkflow,
)


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
    with ThreadPoolExecutor(max_workers=100) as executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[MeetingAnalysisWorkflow, AsyncMeetingAnalysisWorkflow],
            activities=[
                transcript,
                speaker_diarization,
                language,
                silence,
                vector_store,
                unknown_speaker_inference,
                template,
                summary,
                topics_with_highlights,
                keyword_highlights,
                kpis,
                kpis_summary,
                meeting_workflows,
                async_transcript,
                async_speaker_diarization,
                async_language,
                async_silence,
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
        )
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
