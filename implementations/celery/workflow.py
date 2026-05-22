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


def build_workflow():
    return chain(
        # 1. transcript
        transcript.si(),
        # 2. depend on transcript
        group(speaker_diarization.si(), language.si(), silence.si()),
        # 3. depend on speakers + language + silence
        group(vector_store.si(), unknown_speaker_inference.si(), template.si()),
        # 4. depend on level 3; kpis_summary trails kpis without blocking the others
        group(
            summary.si(),
            topics_with_highlights.si(),
            keyword_highlights.si(),
            chain(kpis.si(), kpis_summary.si()),
        ),
        # 5. depend on summary + topics + keywords + kpis_summary
        meeting_workflows.si(),
    )
