import os
import uuid
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, Request
from pydantic import BaseModel
from temporalio.client import Client

from implementations.temporal.activities import MeetingInput
from implementations.temporal.shared import TASK_QUEUE
from implementations.temporal.workflows import (
    AsyncMeetingAnalysisWorkflow,
    MeetingAnalysisWorkflow,
)

WORKFLOWS = {
    "sync": MeetingAnalysisWorkflow,
    "async": AsyncMeetingAnalysisWorkflow,
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.temporal = await Client.connect(
        os.getenv("TEMPORAL_ADDRESS", "temporal:7233")
    )
    yield


app = FastAPI(lifespan=lifespan)


class AnalyzeRequest(BaseModel):
    title: str = "untitled"
    size: int = 0
    failure_rate: float = 0.0
    mode: Literal["sync", "async"] = "async"


@app.post("/analyze")
async def analyze(req: AnalyzeRequest, request: Request) -> dict:
    client: Client = request.app.state.temporal
    meeting: MeetingInput = {
        "title": req.title,
        "size": req.size,
        "failure_rate": req.failure_rate,
    }
    handle = await client.start_workflow(
        WORKFLOWS[req.mode].run,
        meeting,
        id=f"meeting-analysis-{req.mode}-{uuid.uuid4()}",
        task_queue=TASK_QUEUE,
    )
    return {
        "workflow_id": handle.id,
        "run_id": handle.first_execution_run_id,
        "mode": req.mode,
    }
