FROM python:3.12-slim

WORKDIR /app

COPY implementations/celery/worker/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY meetgeek ./meetgeek
COPY implementations/__init__.py ./implementations/__init__.py
COPY implementations/celery ./implementations/celery

ENV PYTHONUNBUFFERED=1

# Analysis stages are OpenAI API calls (I/O-bound) -> gevent green-thread pool, the
# production-appropriate choice (high I/O concurrency, one process). Concurrency kept at
# 20 for parity with the Temporal async worker. (Transcriber stays prefork: CPU-bound.)
CMD ["celery", "-A", "implementations.celery.app", "worker", "--loglevel=info", "--pool", "gevent", "--concurrency=20", "-E", "-Q", "meeting-analysis"]
