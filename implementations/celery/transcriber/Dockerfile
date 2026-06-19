FROM python:3.12-slim

WORKDIR /app

COPY implementations/celery/transcriber/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY meetgeek ./meetgeek
COPY implementations/__init__.py ./implementations/__init__.py
COPY implementations/celery ./implementations/celery

ENV PYTHONUNBUFFERED=1

CMD ["celery", "-A", "implementations.celery.app", "worker", "--loglevel=info", "--concurrency=4", "-E", "-Q", "meeting-transcription"]
