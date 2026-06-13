import asyncio
from datetime import timedelta

from temporalio import workflow

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

TIMEOUT = timedelta(minutes=5)


@workflow.defn
class MeetingAnalysisWorkflow:
    @workflow.run
    async def run(self, meeting: MeetingInput) -> None:
        def act(fn):
            return workflow.execute_activity(fn, start_to_close_timeout=TIMEOUT)

        # 1. transcript
        await act(transcript)

        # 2. depend on transcript
        await asyncio.gather(
            act(speaker_diarization),
            act(language),
            act(silence),
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
        def act(fn):
            return workflow.execute_activity(fn, start_to_close_timeout=TIMEOUT)

        # 1. transcript
        await act(async_transcript)

        # 2. depend on transcript
        await asyncio.gather(
            act(async_speaker_diarization),
            act(async_language),
            act(async_silence),
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
