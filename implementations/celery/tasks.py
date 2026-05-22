from meetgeek.sdk import MeetGeekSDK

from implementations.celery.app import app


@app.task
def transcript() -> None:
    MeetGeekSDK.run_transcript()


@app.task
def speaker_diarization() -> None:
    MeetGeekSDK.run_speaker_diarization()


@app.task
def language() -> None:
    MeetGeekSDK.run_language_identification()


@app.task
def silence() -> None:
    MeetGeekSDK.run_silence_intervals()


@app.task
def vector_store() -> None:
    MeetGeekSDK.run_update_vector_store()


@app.task
def unknown_speaker_inference() -> None:
    MeetGeekSDK.run_infer_unknown_speakers()


@app.task
def template() -> None:
    MeetGeekSDK.run_template()


@app.task
def summary() -> None:
    MeetGeekSDK.run_summary()


@app.task
def topics_with_highlights() -> None:
    MeetGeekSDK.run_topics_and_highlights()


@app.task
def keyword_highlights() -> None:
    MeetGeekSDK.run_keyword_highlights()


@app.task
def kpis() -> None:
    MeetGeekSDK.run_kpis()


@app.task
def kpis_summary() -> None:
    MeetGeekSDK.run_kpis_summary()


@app.task
def meeting_workflows() -> None:
    MeetGeekSDK.run_meeting_workflows()
