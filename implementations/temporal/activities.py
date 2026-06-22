import asyncio
import contextlib
import contextvars
import threading
from typing import TypedDict

from temporalio import activity

from meetgeek import sdk
from implementations.temporal.shared import HEARTBEAT_INTERVAL

# Heartbeat cadence = the shared HEARTBEAT_INTERVAL, kept well below HEARTBEAT_TIMEOUT so the
# server is refreshed with a wide margin even for the longest activity (~700s diarize) and
# under load. Paired with `heartbeat_timeout` on the workflow side: a worker that dies
# mid-activity is detected within the timeout and rescheduled.
HEARTBEAT_EVERY_S = HEARTBEAT_INTERVAL.total_seconds()


class MeetingInput(TypedDict, total=False):
    title: str
    size: int
    failure_rate: float


@contextlib.contextmanager
def heartbeating(every: float = HEARTBEAT_EVERY_S):
    """Emit Temporal heartbeats from a background daemon thread while a blocking sync
    call runs - the standard way to heartbeat long, uninterruptible work that cannot
    itself call `activity.heartbeat()`. `activity.heartbeat()` is thread-safe; the
    activity context is carried into the thread via `contextvars`.

    Production usage (real Whisper transcription):

        @activity.defn
        def transcript(meeting):
            with heartbeating():
                return whisper_model.transcribe(meeting["audio_path"])
    """
    stop = threading.Event()
    ctx = contextvars.copy_context()
    hb = threading.Thread(target=lambda: ctx.run(sync_beat, stop, every), daemon=True)
    hb.start()
    try:
        yield
    finally:
        stop.set()
        hb.join()


def sync_beat(stop: threading.Event, every: float):
    activity.heartbeat()              # immediate first beat - no initial gap
    while not stop.wait(every):
        activity.heartbeat()


@contextlib.asynccontextmanager
async def heartbeating_async(every: float = HEARTBEAT_EVERY_S):
    """Same, for async activities: a concurrent task heartbeats while you await a call.

    Production usage (real OpenAI Responses API):

        @activity.defn
        async def summary(meeting):
            async with heartbeating_async():
                return await openai_client.responses.create(...)
    """
    async def async_beat():
        with contextlib.suppress(asyncio.CancelledError):
            activity.heartbeat()      # immediate first beat - no initial gap
            while True:
                await asyncio.sleep(every)
                activity.heartbeat()

    hb = asyncio.create_task(async_beat())
    try:
        yield
    finally:
        hb.cancel()


# In this simulation the single blocking call inside the wrapper is a sleep modelling the
# stage's measured duration; in production it is the real Whisper / OpenAI call (above).
def run_sync(meeting, model, api):
    secs = sdk.api_duration(meeting, model) if api else sdk.duration(meeting, model)
    with heartbeating():
        sdk.run_stage(meeting, secs)


async def run_async(meeting, model, api):
    secs = sdk.api_duration(meeting, model) if api else sdk.duration(meeting, model)
    async with heartbeating_async():
        await sdk.run_stage_async(meeting, secs)


# --- transcription (Whisper, sync, near-normal duration) ---
@activity.defn
def transcript(meeting: MeetingInput) -> None:
    run_sync(meeting, sdk.TRANSCRIBE, api=False)


@activity.defn
def speaker_diarization(meeting: MeetingInput) -> None:
    run_sync(meeting, sdk.DIARIZE, api=False)


@activity.defn
def language(meeting: MeetingInput) -> None:
    run_sync(meeting, sdk.LANGUAGE, api=False)


@activity.defn
def silence(meeting: MeetingInput) -> None:
    run_sync(meeting, sdk.SILENCE, api=False)


# --- analysis (OpenAI API, sync, lognormal duration) ---
@activity.defn
def vector_store(meeting: MeetingInput) -> None:
    run_sync(meeting, sdk.VECTOR_STORE, api=True)


@activity.defn
def unknown_speaker_inference(meeting: MeetingInput) -> None:
    run_sync(meeting, sdk.INFER_SPEAKERS, api=True)


@activity.defn
def template(meeting: MeetingInput) -> None:
    run_sync(meeting, sdk.TEMPLATE, api=True)


@activity.defn
def summary(meeting: MeetingInput) -> None:
    run_sync(meeting, sdk.SUMMARY, api=True)


@activity.defn
def topics_with_highlights(meeting: MeetingInput) -> None:
    run_sync(meeting, sdk.TOPICS, api=True)


@activity.defn
def keyword_highlights(meeting: MeetingInput) -> None:
    run_sync(meeting, sdk.KEYWORDS, api=True)


@activity.defn
def kpis(meeting: MeetingInput) -> None:
    run_sync(meeting, sdk.KPIS, api=True)


@activity.defn
def kpis_summary(meeting: MeetingInput) -> None:
    run_sync(meeting, sdk.KPIS_SUMMARY, api=True)


@activity.defn
def meeting_workflows(meeting: MeetingInput) -> None:
    run_sync(meeting, sdk.MEETING_WORKFLOWS, api=True)


# --- analysis (OpenAI API, async, lognormal duration) ---
@activity.defn
async def async_vector_store(meeting: MeetingInput) -> None:
    await run_async(meeting, sdk.VECTOR_STORE, api=True)


@activity.defn
async def async_unknown_speaker_inference(meeting: MeetingInput) -> None:
    await run_async(meeting, sdk.INFER_SPEAKERS, api=True)


@activity.defn
async def async_template(meeting: MeetingInput) -> None:
    await run_async(meeting, sdk.TEMPLATE, api=True)


@activity.defn
async def async_summary(meeting: MeetingInput) -> None:
    await run_async(meeting, sdk.SUMMARY, api=True)


@activity.defn
async def async_topics_with_highlights(meeting: MeetingInput) -> None:
    await run_async(meeting, sdk.TOPICS, api=True)


@activity.defn
async def async_keyword_highlights(meeting: MeetingInput) -> None:
    await run_async(meeting, sdk.KEYWORDS, api=True)


@activity.defn
async def async_kpis(meeting: MeetingInput) -> None:
    await run_async(meeting, sdk.KPIS, api=True)


@activity.defn
async def async_kpis_summary(meeting: MeetingInput) -> None:
    await run_async(meeting, sdk.KPIS_SUMMARY, api=True)


@activity.defn
async def async_meeting_workflows(meeting: MeetingInput) -> None:
    await run_async(meeting, sdk.MEETING_WORKFLOWS, api=True)
