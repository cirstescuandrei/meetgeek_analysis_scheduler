import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
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
)
from implementations.temporal.shared import TASK_QUEUE
from implementations.temporal.workflows import MeetingAnalysisWorkflow


async def main() -> None:
    client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"))
    with ThreadPoolExecutor(max_workers=100) as executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[MeetingAnalysisWorkflow],
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
            ],
            activity_executor=executor,
        )
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
