import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from pydantic import BaseModel
from temporalio.client import Client

from implementations.temporal.activities import MeetingInput
from implementations.temporal.shared import TASK_QUEUE
from implementations.temporal.workflows import MeetingAnalysisWorkflow


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.temporal = await Client.connect(
        os.getenv("TEMPORAL_ADDRESS", "temporal:7233")
    )
    yield


app = FastAPI(lifespan=lifespan)


class AnalyzeRequest(BaseModel):
    meeting_id: str | None = None
    audio_path: str = "sample.wav"


@app.post("/analyze")
async def analyze(req: AnalyzeRequest, request: Request) -> dict:
    client: Client = request.app.state.temporal
    meeting_id = req.meeting_id or str(uuid.uuid4())
    handle = await client.start_workflow(
        MeetingAnalysisWorkflow.run,
        MeetingInput(meeting_id=meeting_id, audio_path=req.audio_path),
        id=f"meeting-analysis-{uuid.uuid4()}",
        task_queue=TASK_QUEUE,
    )
    return {"workflow_id": handle.id, "run_id": handle.first_execution_run_id}
