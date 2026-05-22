FROM python:3.12-slim

WORKDIR /app

COPY implementations/temporal/client/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY meetgeek ./meetgeek
COPY implementations/__init__.py ./implementations/__init__.py
COPY implementations/temporal ./implementations/temporal

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "implementations.temporal.client.app:app", "--host", "0.0.0.0", "--port", "8000"]
