import asyncio
import math
import os
import random
import time

# Transcription stages (local Whisper inference)
# CPU/GPU-bound, roughly linear in audio length. Modelled by linear regression on
# real timing data (meetings <=200MB): mean_seconds = slope*size_mb + intercept,
# sampled from a near-normal distribution.
TRANSCRIBE = (1.25, 36.0)   # transcribe_sentences  (R2 0.29)
DIARIZE = (3.36, 34.0)      # diarize_speakers      (R2 0.69)
LANGUAGE = (0.14, 30.0)     # detect_language       (R2 0.01, ~size-independent)
SILENCE = (1.09, 37.0)      # silence_intervals     (R2 0.15)

# Analysis stages (OpenAI Responses API calls + result post-processing)
# Network-I/O-bound LLM calls. Duration is dominated by output-token generation
# (roughly bounded per task) plus a fixed API/RTT overhead, with only weak scaling
# in input tokens (based on transcript length/meeting size).
#
# Modelled as base_s + slope*size_mb and sampled from a LOGNORMAL (right-skewed) to mimic LLM latency tails.
# We have limited timing data for these - values are reasoned estimates (document as such).
#                       (base_s, slope_s_per_mb)
VECTOR_STORE = (3.0, 0.07)      # embed transcript chunks + upsert; scales most with length
INFER_SPEAKERS = (3.0, 0.02)    # one LLM call to name unknown speakers
TEMPLATE = (2.5, 0.01)          # template selection; light, near size-independent
SUMMARY = (6.0, 0.06)           # large input context + long generated summary
TOPICS = (6.0, 0.05)            # topics + highlights, structured long output
KEYWORDS = (3.0, 0.03)          # keyword extraction, shorter output
KPIS = (4.0, 0.03)              # structured KPI extraction
KPIS_SUMMARY = (2.5, 0.0)       # summarises the KPI output; small fixed input
MEETING_WORKFLOWS = (4.0, 0.02) # action-items / workflow generation

SIGMA_FRACTION = 0.15       # transcription: gaussian noise around the modelled mean
API_CV = 0.40               # analysis: lognormal coefficient of variation (LLM latency spread)
API_OVERHEAD_S = 6.0        # analysis: fixed network + result-aggregation delay added to every API task, independent of meeting size
SIZE_CAP_MB = 200.0         # models are only valid in the fitted/estimated range of 200MB
BYTES_PER_MB = 1024 * 1024

# A failed attempt fails partway through the stage, after running this fraction of its
# duration. Modelling a flaky call that errors mid-flight (a timeout or a dropped
# connection) charges the wasted time to wall-clock, so a retry costs real work.
FAILURE_WORK_FRACTION = (0.2, 1.0)

# Global multiplier to modify workflow speed
DURATION_SCALE = float(os.getenv("DURATION_SCALE", "1.0"))


def size_mb(meeting):
    return min(max(meeting.get("size", 0), 0) / BYTES_PER_MB, SIZE_CAP_MB)


def duration(meeting, model):
    """
        Transcription (Whisper): near-normal around a size-linear mean.
    """
    slope, intercept = model
    mean = (slope * size_mb(meeting) + intercept) * DURATION_SCALE
    return max(0.1, random.gauss(mean, abs(mean) * SIGMA_FRACTION))


def api_duration(meeting, model):
    """
        Analysis (OpenAI API): right-skewed lognormal around a size-linear mean,
        plus a fixed network/aggregation overhead independent of meeting size.
    """
    base, slope = model
    mean = (API_OVERHEAD_S + base + slope * size_mb(meeting)) * DURATION_SCALE
    sigma = math.sqrt(math.log(1 + API_CV ** 2))
    mu = math.log(max(mean, 1e-3)) - sigma ** 2 / 2
    return max(0.1, random.lognormvariate(mu, sigma))


def failure_fraction(meeting):
    """With probability failure_rate, return the fraction of the stage to run before
    failing; otherwise None. Each invocation decides independently, so retries usually
    recover."""
    rate = meeting.get("failure_rate", 0.0)
    if rate and random.random() < rate:
        return random.uniform(*FAILURE_WORK_FRACTION)
    return None


def run_stage(meeting, secs):
    """Sleep for the stage duration, or fail partway through after wasting part of it."""
    frac = failure_fraction(meeting)
    if frac is None:
        time.sleep(secs)
        return
    time.sleep(secs * frac)
    raise RuntimeError(f"random failure (p={meeting.get('failure_rate')}): {meeting.get('title')!r}")


async def run_stage_async(meeting, secs):
    """Async variant of run_stage."""
    frac = failure_fraction(meeting)
    if frac is None:
        await asyncio.sleep(secs)
        return
    await asyncio.sleep(secs * frac)
    raise RuntimeError(f"random failure (p={meeting.get('failure_rate')}): {meeting.get('title')!r}")


class MeetGeekSDK:
    """Mock client for the MeetGeek API"""

    # Transcription Mock (Whisper)
    @classmethod
    def run_transcript(cls, meeting):
        run_stage(meeting, duration(meeting, TRANSCRIBE))

    @classmethod
    def run_speaker_diarization(cls, meeting):
        run_stage(meeting, duration(meeting, DIARIZE))

    @classmethod
    def run_language_identification(cls, meeting):
        run_stage(meeting, duration(meeting, LANGUAGE))

    @classmethod
    def run_silence_intervals(cls, meeting):
        run_stage(meeting, duration(meeting, SILENCE))

    # Analysis Mock (OpenAI Responses API)
    @classmethod
    def run_update_vector_store(cls, meeting):
        run_stage(meeting, api_duration(meeting, VECTOR_STORE))

    @classmethod
    def run_infer_unknown_speakers(cls, meeting):
        run_stage(meeting, api_duration(meeting, INFER_SPEAKERS))

    @classmethod
    def run_template(cls, meeting):
        run_stage(meeting, api_duration(meeting, TEMPLATE))

    @classmethod
    def run_summary(cls, meeting):
        run_stage(meeting, api_duration(meeting, SUMMARY))

    @classmethod
    def run_topics_and_highlights(cls, meeting):
        run_stage(meeting, api_duration(meeting, TOPICS))

    @classmethod
    def run_keyword_highlights(cls, meeting):
        run_stage(meeting, api_duration(meeting, KEYWORDS))

    @classmethod
    def run_kpis(cls, meeting):
        run_stage(meeting, api_duration(meeting, KPIS))

    @classmethod
    def run_kpis_summary(cls, meeting):
        run_stage(meeting, api_duration(meeting, KPIS_SUMMARY))

    @classmethod
    def run_meeting_workflows(cls, meeting):
        run_stage(meeting, api_duration(meeting, MEETING_WORKFLOWS))


class AsyncMeetGeekSDK:
    """Async mock client for the MeetGeek API"""

    # Transcription Mock (Whisper)
    @classmethod
    async def run_transcript(cls, meeting):
        await run_stage_async(meeting, duration(meeting, TRANSCRIBE))

    @classmethod
    async def run_speaker_diarization(cls, meeting):
        await run_stage_async(meeting, duration(meeting, DIARIZE))

    @classmethod
    async def run_language_identification(cls, meeting):
        await run_stage_async(meeting, duration(meeting, LANGUAGE))

    @classmethod
    async def run_silence_intervals(cls, meeting):
        await run_stage_async(meeting, duration(meeting, SILENCE))

    # Analysis Mock (OpenAI Responses API)
    @classmethod
    async def run_update_vector_store(cls, meeting):
        await run_stage_async(meeting, api_duration(meeting, VECTOR_STORE))

    @classmethod
    async def run_infer_unknown_speakers(cls, meeting):
        await run_stage_async(meeting, api_duration(meeting, INFER_SPEAKERS))

    @classmethod
    async def run_template(cls, meeting):
        await run_stage_async(meeting, api_duration(meeting, TEMPLATE))

    @classmethod
    async def run_summary(cls, meeting):
        await run_stage_async(meeting, api_duration(meeting, SUMMARY))

    @classmethod
    async def run_topics_and_highlights(cls, meeting):
        await run_stage_async(meeting, api_duration(meeting, TOPICS))

    @classmethod
    async def run_keyword_highlights(cls, meeting):
        await run_stage_async(meeting, api_duration(meeting, KEYWORDS))

    @classmethod
    async def run_kpis(cls, meeting):
        await run_stage_async(meeting, api_duration(meeting, KPIS))

    @classmethod
    async def run_kpis_summary(cls, meeting):
        await run_stage_async(meeting, api_duration(meeting, KPIS_SUMMARY))

    @classmethod
    async def run_meeting_workflows(cls, meeting):
        await run_stage_async(meeting, api_duration(meeting, MEETING_WORKFLOWS))
