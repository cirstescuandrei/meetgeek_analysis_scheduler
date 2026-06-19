from meetgeek.sdk import MeetGeekSDK

from implementations.celery.app import app


@app.task
def transcript(meeting: dict) -> None:
    MeetGeekSDK.run_transcript(meeting)


@app.task
def speaker_diarization(meeting: dict) -> None:
    MeetGeekSDK.run_speaker_diarization(meeting)


@app.task
def language(meeting: dict) -> None:
    MeetGeekSDK.run_language_identification(meeting)


@app.task
def silence(meeting: dict) -> None:
    MeetGeekSDK.run_silence_intervals(meeting)


@app.task
def vector_store(meeting: dict) -> None:
    MeetGeekSDK.run_update_vector_store(meeting)


@app.task
def unknown_speaker_inference(meeting: dict) -> None:
    MeetGeekSDK.run_infer_unknown_speakers(meeting)


@app.task
def template(meeting: dict) -> None:
    MeetGeekSDK.run_template(meeting)


@app.task
def summary(meeting: dict) -> None:
    MeetGeekSDK.run_summary(meeting)


@app.task
def topics_with_highlights(meeting: dict) -> None:
    MeetGeekSDK.run_topics_and_highlights(meeting)


@app.task
def keyword_highlights(meeting: dict) -> None:
    MeetGeekSDK.run_keyword_highlights(meeting)


@app.task
def kpis(meeting: dict) -> None:
    MeetGeekSDK.run_kpis(meeting)


@app.task
def kpis_summary(meeting: dict) -> None:
    MeetGeekSDK.run_kpis_summary(meeting)


@app.task
def meeting_workflows(meeting: dict) -> None:
    MeetGeekSDK.run_meeting_workflows(meeting)
