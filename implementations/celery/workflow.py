from celery import chain, group

from implementations.celery.tasks import (
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
    unknown_speaker_inference,
    vector_store,
)


def build_workflow(meeting: dict):
    return chain(
        # 1. transcript
        transcript.si(meeting),
        # 2. depend on transcript
        group(
            speaker_diarization.si(meeting),
            language.si(meeting),
            silence.si(meeting),
        ),
        # 3. depend on speakers + language + silence
        group(
            vector_store.si(meeting),
            unknown_speaker_inference.si(meeting),
            template.si(meeting),
        ),
        # 4. depend on level 3; kpis_summary trails kpis without blocking the others
        group(
            summary.si(meeting),
            topics_with_highlights.si(meeting),
            keyword_highlights.si(meeting),
            chain(kpis.si(meeting), kpis_summary.si(meeting)),
        ),
        # 5. depend on summary + topics + keywords + kpis_summary
        meeting_workflows.si(meeting),
    )
