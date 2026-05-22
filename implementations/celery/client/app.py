import uuid

from fastapi import FastAPI
from pydantic import BaseModel

from implementations.celery.workflow import build_workflow

app = FastAPI()


class AnalyzeRequest(BaseModel):
    meeting_id: str | None = None
    audio_path: str = "sample.wav"


@app.post("/analyze")
def analyze(req: AnalyzeRequest) -> dict:
    meeting_id = req.meeting_id or str(uuid.uuid4())
    result = build_workflow().apply_async()
    return {"meeting_id": meeting_id, "task_id": result.id}
