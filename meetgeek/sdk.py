import random
import time


def jitter(value=5.0, pct=0.5):
    return value * random.uniform(1 - pct, 1 + pct)


class MeetGeekSDK:
    """Mock client for the MeetGeek API"""

    @classmethod
    def run_transcript(cls):
        time.sleep(jitter())

    @classmethod
    def run_speaker_diarization(cls):
        time.sleep(jitter())

    @classmethod
    def run_language_identification(cls):
        time.sleep(jitter())

    @classmethod
    def run_silence_intervals(cls):
        time.sleep(jitter())

    @classmethod
    def run_update_vector_store(cls):
        time.sleep(jitter())

    @classmethod
    def run_infer_unknown_speakers(cls):
        time.sleep(jitter())

    @classmethod
    def run_template(cls):
        time.sleep(jitter())

    @classmethod
    def run_summary(cls):
        time.sleep(jitter())

    @classmethod
    def run_topics_and_highlights(cls):
        time.sleep(jitter())

    @classmethod
    def run_keyword_highlights(cls):
        time.sleep(jitter())

    @classmethod
    def run_kpis(cls):
        time.sleep(jitter())

    @classmethod
    def run_kpis_summary(cls):
        time.sleep(jitter())

    @classmethod
    def run_meeting_workflows(cls):
        time.sleep(jitter())