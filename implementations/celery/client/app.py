from fastapi import FastAPI
from pydantic import BaseModel

from implementations.celery.workflow import build_workflow

app = FastAPI()


class AnalyzeRequest(BaseModel):
    title: str = "untitled"
    size: int = 0
    should_fail: bool = False


@app.post("/analyze")
def analyze(req: AnalyzeRequest) -> dict:
    meeting = {
        "title": req.title,
        "size": req.size,
        "should_fail": req.should_fail,
    }
    result = build_workflow(meeting).apply_async()
    return {"task_id": result.id}
