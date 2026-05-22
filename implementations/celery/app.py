import os

from celery import Celery

app = Celery(
    "meetgeek",
    broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0"),
    include=["implementations.celery.tasks"],
)
app.conf.task_default_queue = "meeting-analysis"
