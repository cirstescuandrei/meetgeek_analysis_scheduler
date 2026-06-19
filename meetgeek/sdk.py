import asyncio
import random
import time

SIZE_CAP_BYTES = 100 * 1024 * 1024
MIN_DURATION_SEC = 1.0
MAX_DURATION_SEC = 30.0
SIGMA_FRACTION = 0.15


def maybe_fail(meeting):
    if meeting.get("should_fail"):
        raise RuntimeError(f"forced failure: {meeting.get('title')!r}")


def duration(meeting):
    size = min(max(meeting.get("size", 0), 0), SIZE_CAP_BYTES)
    mean = MIN_DURATION_SEC + (size / SIZE_CAP_BYTES) * (
        MAX_DURATION_SEC - MIN_DURATION_SEC
    )
    return max(0.1, random.gauss(mean, mean * SIGMA_FRACTION))


class MeetGeekSDK:
    """Mock client for the MeetGeek API"""

    @classmethod
    def run_transcript(cls, meeting):
        maybe_fail(meeting)
        time.sleep(duration(meeting))

    @classmethod
    def run_speaker_diarization(cls, meeting):
        maybe_fail(meeting)
        time.sleep(duration(meeting))

    @classmethod
    def run_language_identification(cls, meeting):
        maybe_fail(meeting)
        time.sleep(duration(meeting))

    @classmethod
    def run_silence_intervals(cls, meeting):
        maybe_fail(meeting)
        time.sleep(duration(meeting))

    @classmethod
    def run_update_vector_store(cls, meeting):
        maybe_fail(meeting)
        time.sleep(duration(meeting))

    @classmethod
    def run_infer_unknown_speakers(cls, meeting):
        maybe_fail(meeting)
        time.sleep(duration(meeting))

    @classmethod
    def run_template(cls, meeting):
        maybe_fail(meeting)
        time.sleep(duration(meeting))

    @classmethod
    def run_summary(cls, meeting):
        maybe_fail(meeting)
        time.sleep(duration(meeting))

    @classmethod
    def run_topics_and_highlights(cls, meeting):
        maybe_fail(meeting)
        time.sleep(duration(meeting))

    @classmethod
    def run_keyword_highlights(cls, meeting):
        maybe_fail(meeting)
        time.sleep(duration(meeting))

    @classmethod
    def run_kpis(cls, meeting):
        maybe_fail(meeting)
        time.sleep(duration(meeting))

    @classmethod
    def run_kpis_summary(cls, meeting):
        maybe_fail(meeting)
        time.sleep(duration(meeting))

    @classmethod
    def run_meeting_workflows(cls, meeting):
        maybe_fail(meeting)
        time.sleep(duration(meeting))


class AsyncMeetGeekSDK:
    """Async mock client for the MeetGeek API"""

    @classmethod
    async def run_transcript(cls, meeting):
        maybe_fail(meeting)
        await asyncio.sleep(duration(meeting))

    @classmethod
    async def run_speaker_diarization(cls, meeting):
        maybe_fail(meeting)
        await asyncio.sleep(duration(meeting))

    @classmethod
    async def run_language_identification(cls, meeting):
        maybe_fail(meeting)
        await asyncio.sleep(duration(meeting))

    @classmethod
    async def run_silence_intervals(cls, meeting):
        maybe_fail(meeting)
        await asyncio.sleep(duration(meeting))

    @classmethod
    async def run_update_vector_store(cls, meeting):
        maybe_fail(meeting)
        await asyncio.sleep(duration(meeting))

    @classmethod
    async def run_infer_unknown_speakers(cls, meeting):
        maybe_fail(meeting)
        await asyncio.sleep(duration(meeting))

    @classmethod
    async def run_template(cls, meeting):
        maybe_fail(meeting)
        await asyncio.sleep(duration(meeting))

    @classmethod
    async def run_summary(cls, meeting):
        maybe_fail(meeting)
        await asyncio.sleep(duration(meeting))

    @classmethod
    async def run_topics_and_highlights(cls, meeting):
        maybe_fail(meeting)
        await asyncio.sleep(duration(meeting))

    @classmethod
    async def run_keyword_highlights(cls, meeting):
        maybe_fail(meeting)
        await asyncio.sleep(duration(meeting))

    @classmethod
    async def run_kpis(cls, meeting):
        maybe_fail(meeting)
        await asyncio.sleep(duration(meeting))

    @classmethod
    async def run_kpis_summary(cls, meeting):
        maybe_fail(meeting)
        await asyncio.sleep(duration(meeting))

    @classmethod
    async def run_meeting_workflows(cls, meeting):
        maybe_fail(meeting)
        await asyncio.sleep(duration(meeting))
