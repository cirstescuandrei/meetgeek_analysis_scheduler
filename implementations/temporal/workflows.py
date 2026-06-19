import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from implementations.temporal.activities import (
        MeetingInput,
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
        async_meeting_workflows,
        async_summary,
        async_template,
        async_topics_with_highlights,
        async_vector_store,
    )
    from implementations.temporal.shared import TRANSCRIBER_TASK_QUEUE

TIMEOUT = timedelta(minutes=5)
RETRY_POLICY = RetryPolicy(maximum_attempts=5)


@workflow.defn
class MeetingAnalysisWorkflow:
    @workflow.run
    async def run(self, meeting: MeetingInput) -> None:
        def act(fn, queue=None):
            if queue:
                return workflow.execute_activity(
                    fn,
                    meeting,
                    start_to_close_timeout=TIMEOUT,
                    task_queue=queue,
                    retry_policy=RETRY_POLICY,
                )
            return workflow.execute_activity(
                fn,
                meeting,
                start_to_close_timeout=TIMEOUT,
                retry_policy=RETRY_POLICY,
            )

        # 1. transcript (transcriber queue)
        await act(transcript, queue=TRANSCRIBER_TASK_QUEUE)

        # 2. depend on transcript (transcriber queue)
        await asyncio.gather(
            act(speaker_diarization, queue=TRANSCRIBER_TASK_QUEUE),
            act(language, queue=TRANSCRIBER_TASK_QUEUE),
            act(silence, queue=TRANSCRIBER_TASK_QUEUE),
        )

        # 3. depend on speakers + language + silence
        vector_store_task = asyncio.create_task(act(vector_store))
        infer_speakers_task = asyncio.create_task(act(unknown_speaker_inference))
        template_task = asyncio.create_task(act(template))

        # 4. depend on infer_speakers + template
        await asyncio.gather(
            vector_store_task,
            infer_speakers_task,
            template_task,
        )
        summary_task = asyncio.create_task(act(summary))
        topics_task = asyncio.create_task(act(topics_with_highlights))
        keywords_task = asyncio.create_task(act(keyword_highlights))
        kpis_task = asyncio.create_task(act(kpis))

        # 5. kpis_summary depends on kpis
        await kpis_task
        kpis_summary_task = asyncio.create_task(act(kpis_summary))

        # 6. depend on summary + topics + keywords + kpis_summary
        await asyncio.gather(
            summary_task,
            topics_task,
            keywords_task,
            kpis_summary_task,
        )
        await act(meeting_workflows)


@workflow.defn
class AsyncMeetingAnalysisWorkflow:
    @workflow.run
    async def run(self, meeting: MeetingInput) -> None:
        def act(fn, queue=None):
            if queue:
                return workflow.execute_activity(
                    fn,
                    meeting,
                    start_to_close_timeout=TIMEOUT,
                    task_queue=queue,
                    retry_policy=RETRY_POLICY,
                )
            return workflow.execute_activity(
                fn,
                meeting,
                start_to_close_timeout=TIMEOUT,
                retry_policy=RETRY_POLICY,
            )

        # 1. transcript (sync, transcriber queue)
        await act(transcript, queue=TRANSCRIBER_TASK_QUEUE)

        # 2. depend on transcript (sync, transcriber queue)
        await asyncio.gather(
            act(speaker_diarization, queue=TRANSCRIBER_TASK_QUEUE),
            act(language, queue=TRANSCRIBER_TASK_QUEUE),
            act(silence, queue=TRANSCRIBER_TASK_QUEUE),
        )

        # 3. depend on speakers + language + silence
        vector_store_task = asyncio.create_task(act(async_vector_store))
        infer_speakers_task = asyncio.create_task(act(async_unknown_speaker_inference))
        template_task = asyncio.create_task(act(async_template))

        # 4. depend on infer_speakers + template
        await asyncio.gather(
            vector_store_task,
            infer_speakers_task,
            template_task,
        )
        summary_task = asyncio.create_task(act(async_summary))
        topics_task = asyncio.create_task(act(async_topics_with_highlights))
        keywords_task = asyncio.create_task(act(async_keyword_highlights))
        kpis_task = asyncio.create_task(act(async_kpis))

        # 5. kpis_summary depends on kpis
        await kpis_task
        kpis_summary_task = asyncio.create_task(act(async_kpis_summary))

        # 6. depend on summary + topics + keywords + kpis_summary
        await asyncio.gather(
            summary_task,
            topics_task,
            keywords_task,
            kpis_summary_task,
        )
        await act(async_meeting_workflows)
