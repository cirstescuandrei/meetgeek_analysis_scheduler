import time

from meetgeek.sdk import MeetGeekSDK

from implementations.celery.app import app
from implementations.celery.metrics import WORKFLOW_E2E

# Mirrors Temporal's RetryPolicy(maximum_attempts=5): 1 initial + 4 retries.
RETRY_OPTS = dict(
    autoretry_for=(Exception,),
    max_retries=4,
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
)


@app.task(**RETRY_OPTS)
def transcript(meeting: dict) -> None:
    MeetGeekSDK.run_transcript(meeting)


@app.task(**RETRY_OPTS)
def speaker_diarization(meeting: dict) -> None:
    MeetGeekSDK.run_speaker_diarization(meeting)


@app.task(**RETRY_OPTS)
def language(meeting: dict) -> None:
    MeetGeekSDK.run_language_identification(meeting)


@app.task(**RETRY_OPTS)
def silence(meeting: dict) -> None:
    MeetGeekSDK.run_silence_intervals(meeting)


@app.task(**RETRY_OPTS)
def vector_store(meeting: dict) -> None:
    MeetGeekSDK.run_update_vector_store(meeting)


@app.task(**RETRY_OPTS)
def unknown_speaker_inference(meeting: dict) -> None:
    MeetGeekSDK.run_infer_unknown_speakers(meeting)


@app.task(**RETRY_OPTS)
def template(meeting: dict) -> None:
    MeetGeekSDK.run_template(meeting)


@app.task(**RETRY_OPTS)
def summary(meeting: dict) -> None:
    MeetGeekSDK.run_summary(meeting)


@app.task(**RETRY_OPTS)
def topics_with_highlights(meeting: dict) -> None:
    MeetGeekSDK.run_topics_and_highlights(meeting)


@app.task(**RETRY_OPTS)
def keyword_highlights(meeting: dict) -> None:
    MeetGeekSDK.run_keyword_highlights(meeting)


@app.task(**RETRY_OPTS)
def kpis(meeting: dict) -> None:
    MeetGeekSDK.run_kpis(meeting)


@app.task(**RETRY_OPTS)
def kpis_summary(meeting: dict) -> None:
    MeetGeekSDK.run_kpis_summary(meeting)


@app.task(**RETRY_OPTS)
def meeting_workflows(meeting: dict) -> None:
    MeetGeekSDK.run_meeting_workflows(meeting)
    # Final task: record whole-workflow latency (submit -> now) for the harness, mirroring
    # Temporal's native e2e-latency histogram. submitted_at is stamped by the client.
    submitted_at = meeting.get("submitted_at")
    if submitted_at:
        WORKFLOW_E2E.observe(time.time() - submitted_at)
