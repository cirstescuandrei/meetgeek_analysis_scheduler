import os

from celery import Celery

TASK_QUEUE = "meeting-analysis"
TRANSCRIBER_TASK_QUEUE = "meeting-transcription"

TRANSCRIBER_TASKS = (
    "implementations.celery.tasks.transcript",
    "implementations.celery.tasks.speaker_diarization",
    "implementations.celery.tasks.language",
    "implementations.celery.tasks.silence",
)

app = Celery(
    "meetgeek",
    broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0"),
    include=["implementations.celery.tasks"],
)
app.conf.task_default_queue = TASK_QUEUE
app.conf.task_routes = {
    name: {"queue": TRANSCRIBER_TASK_QUEUE} for name in TRANSCRIBER_TASKS
}
app.conf.worker_prefetch_multiplier = 1
app.conf.task_acks_late = True
