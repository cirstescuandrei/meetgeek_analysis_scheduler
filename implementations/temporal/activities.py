from dataclasses import dataclass

from temporalio import activity

from meetgeek.sdk import MeetGeekSDK


@dataclass
class MeetingInput:
    meeting_id: str
    audio_path: str


@activity.defn
def transcript() -> None:
    MeetGeekSDK.run_transcript()


@activity.defn
def speaker_diarization() -> None:
    MeetGeekSDK.run_speaker_diarization()


@activity.defn
def language() -> None:
    MeetGeekSDK.run_language_identification()


@activity.defn
def silence() -> None:
    MeetGeekSDK.run_silence_intervals()


@activity.defn
def vector_store() -> None:
    MeetGeekSDK.run_update_vector_store()


@activity.defn
def unknown_speaker_inference() -> None:
    MeetGeekSDK.run_infer_unknown_speakers()


@activity.defn
def template() -> None:
    MeetGeekSDK.run_template()


@activity.defn
def summary() -> None:
    MeetGeekSDK.run_summary()


@activity.defn
def topics_with_highlights() -> None:
    MeetGeekSDK.run_topics_and_highlights()


@activity.defn
def keyword_highlights() -> None:
    MeetGeekSDK.run_keyword_highlights()


@activity.defn
def kpis() -> None:
    MeetGeekSDK.run_kpis()


@activity.defn
def kpis_summary() -> None:
    MeetGeekSDK.run_kpis_summary()


@activity.defn
def meeting_workflows() -> None:
    MeetGeekSDK.run_meeting_workflows()
