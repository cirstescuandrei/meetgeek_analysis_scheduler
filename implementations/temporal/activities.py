from typing import TypedDict

from temporalio import activity

from meetgeek.sdk import AsyncMeetGeekSDK, MeetGeekSDK


class MeetingInput(TypedDict, total=False):
    title: str
    size: int
    should_fail: bool


@activity.defn
def transcript(meeting: MeetingInput) -> None:
    MeetGeekSDK.run_transcript(meeting)


@activity.defn
def speaker_diarization(meeting: MeetingInput) -> None:
    MeetGeekSDK.run_speaker_diarization(meeting)


@activity.defn
def language(meeting: MeetingInput) -> None:
    MeetGeekSDK.run_language_identification(meeting)


@activity.defn
def silence(meeting: MeetingInput) -> None:
    MeetGeekSDK.run_silence_intervals(meeting)


@activity.defn
def vector_store(meeting: MeetingInput) -> None:
    MeetGeekSDK.run_update_vector_store(meeting)


@activity.defn
def unknown_speaker_inference(meeting: MeetingInput) -> None:
    MeetGeekSDK.run_infer_unknown_speakers(meeting)


@activity.defn
def template(meeting: MeetingInput) -> None:
    MeetGeekSDK.run_template(meeting)


@activity.defn
def summary(meeting: MeetingInput) -> None:
    MeetGeekSDK.run_summary(meeting)


@activity.defn
def topics_with_highlights(meeting: MeetingInput) -> None:
    MeetGeekSDK.run_topics_and_highlights(meeting)


@activity.defn
def keyword_highlights(meeting: MeetingInput) -> None:
    MeetGeekSDK.run_keyword_highlights(meeting)


@activity.defn
def kpis(meeting: MeetingInput) -> None:
    MeetGeekSDK.run_kpis(meeting)


@activity.defn
def kpis_summary(meeting: MeetingInput) -> None:
    MeetGeekSDK.run_kpis_summary(meeting)


@activity.defn
def meeting_workflows(meeting: MeetingInput) -> None:
    MeetGeekSDK.run_meeting_workflows(meeting)


@activity.defn
async def async_vector_store(meeting: MeetingInput) -> None:
    await AsyncMeetGeekSDK.run_update_vector_store(meeting)


@activity.defn
async def async_unknown_speaker_inference(meeting: MeetingInput) -> None:
    await AsyncMeetGeekSDK.run_infer_unknown_speakers(meeting)


@activity.defn
async def async_template(meeting: MeetingInput) -> None:
    await AsyncMeetGeekSDK.run_template(meeting)


@activity.defn
async def async_summary(meeting: MeetingInput) -> None:
    await AsyncMeetGeekSDK.run_summary(meeting)


@activity.defn
async def async_topics_with_highlights(meeting: MeetingInput) -> None:
    await AsyncMeetGeekSDK.run_topics_and_highlights(meeting)


@activity.defn
async def async_keyword_highlights(meeting: MeetingInput) -> None:
    await AsyncMeetGeekSDK.run_keyword_highlights(meeting)


@activity.defn
async def async_kpis(meeting: MeetingInput) -> None:
    await AsyncMeetGeekSDK.run_kpis(meeting)


@activity.defn
async def async_kpis_summary(meeting: MeetingInput) -> None:
    await AsyncMeetGeekSDK.run_kpis_summary(meeting)


@activity.defn
async def async_meeting_workflows(meeting: MeetingInput) -> None:
    await AsyncMeetGeekSDK.run_meeting_workflows(meeting)
