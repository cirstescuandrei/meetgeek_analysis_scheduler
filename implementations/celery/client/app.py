import time

from fastapi import FastAPI
from pydantic import BaseModel

from implementations.celery.workflow import build_workflow

app = FastAPI()


class AnalyzeRequest(BaseModel):
    title: str = "untitled"
    size: int = 0
    failure_rate: float = 0.0


@app.post("/analyze")
def analyze(req: AnalyzeRequest) -> dict:
    meeting = {
        "title": req.title,
        "size": req.size,
        "failure_rate": req.failure_rate,
        # Submit time, carried through the chain so the final task can record the
        # end-to-end workflow latency (mirrors Temporal's native e2e metric).
        "submitted_at": time.time(),
    }
    result = build_workflow(meeting).apply_async()
    return {"task_id": result.id}
