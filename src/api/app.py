from fastapi import FastAPI
from typing import Dict
import asyncio

from ..utils.config import Config
from ..pipeline.etl_pipeline import ETLPipeline

app = FastAPI(title="Australian Company Pipeline API")


@app.on_event("startup")
async def startup_event():
    # Warm up config and pipeline
    app.state.config = Config()
    app.state.pipeline = ETLPipeline(app.state.config)


@app.get("/api/status")
async def get_status() -> Dict:
    return await app.state.pipeline.get_pipeline_status()


@app.get("/api/runs")
async def get_runs(limit: int = 10):
    return await app.state.pipeline.get_recent_runs(limit)


@app.post("/api/run")
async def trigger_run(incremental: bool = False):
    # Fire-and-forget kickoff; return ack immediately
    async def kickoff():
        try:
            await app.state.pipeline.run_full_pipeline(incremental=incremental)
        except Exception:
            pass

    asyncio.create_task(kickoff())
    return {"status": "started", "incremental": incremental}


