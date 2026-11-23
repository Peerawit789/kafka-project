import os
import uuid
import logging
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from dotenv import load_dotenv

load_dotenv()

from database import get_db, init_db
from models import JobModel, JobCreateRequest, JobCreateResponse, JobDetailResponse
from actions import validate_action_request, ACTION_WHITELIST, ALLOWED_ROUTERS
from kafka_producer import producer_instance
from kafka_consumer import start_results_consumer
from websocket_manager import ws_manager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("router-backend")

consumer_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Initializing database...")
    init_db()

    # Start Kafka results consumer in the background
    logger.info("Starting Kafka results consumer background task...")
    global consumer_task
    consumer_task = asyncio.create_task(start_results_consumer())

    yield

    # Shutdown
    logger.info("Shutting down...")
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    producer_instance.flush(timeout=3.0)


app = FastAPI(
    title="Router Automation via Kafka API",
    description="Asynchronous router automation platform with Kafka and GNS3 integration",
    version="1.0.0",
    lifespan=lifespan
)

# Enable CORS for external frontend or development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/actions")
def get_available_actions():
    """Return available routers and whitelisted actions for UI forms."""
    return {
        "routers": sorted(list(ALLOWED_ROUTERS)),
        "actions": ACTION_WHITELIST
    }


@app.post("/api/jobs", response_model=JobCreateResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_job(request: JobCreateRequest, db: Session = Depends(get_db)):
    """
    Validate requested action against whitelist, persist job state as 'pending',
    produce message into Kafka topic router-commands with router_id key,
    and return job_id immediately (<200ms).
    """
    # 1. Whitelist validation
    is_valid, error_msg = validate_action_request(
        router_id=request.router_id,
        action=request.action,
        params=request.params
    )
    if not is_valid:
        raise HTTPException(status_code=400, detail=error_msg)

    job_id = str(uuid.uuid4())
    now = datetime.utcnow()
    iso_now = now.isoformat() + "Z"

    # 2. Persist initial pending state in database
    job_record = JobModel(
        job_id=job_id,
        router_id=request.router_id,
        action=request.action,
        params=request.params,
        requested_by=request.requested_by or "web-user",
        status="pending",
        created_at=now,
    )
    db.add(job_record)
    db.commit()

    # 3. Prepare Kafka message payload according to Section 4.1
    message_payload = {
        "job_id": job_id,
        "router_id": request.router_id,
        "action": request.action,
        "params": request.params,
        "requested_by": request.requested_by or "web-user",
        "created_at": iso_now,
    }

    # 4. Produce to Kafka with router_id as message key
    try:
        producer_instance.produce_command(message_payload)
    except Exception as e:
        logger.error(f"Failed to produce message to Kafka: {e}")
        job_record.status = "failed"
        job_record.error = f"Kafka dispatch failure: {str(e)}"
        db.commit()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit command to message broker: {str(e)}"
        )

    # 5. Broadcast new job event to connected WebSocket clients
    await ws_manager.broadcast({
        "event": "job_created",
        "data": job_record.to_dict()
    })

    return JobCreateResponse(
        job_id=job_id,
        router_id=request.router_id,
        action=request.action,
        status="pending",
        created_at=iso_now,
        message="Command queued successfully for processing"
    )


@app.get("/api/jobs/{job_id}", response_model=JobDetailResponse)
def get_job(job_id: str, db: Session = Depends(get_db)):
    """Fetch status and output of a specific job by job_id."""
    job = db.query(JobModel).filter(JobModel.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return job.to_dict()


@app.get("/api/jobs")
def list_jobs(limit: int = 50, db: Session = Depends(get_db)):
    """List recent jobs ordered by creation timestamp."""
    jobs = db.query(JobModel).order_by(JobModel.created_at.desc()).limit(limit).all()
    return [job.to_dict() for job in jobs]


@app.websocket("/ws/jobs")
async def websocket_jobs_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time status and output streaming to frontend."""
    await ws_manager.connect(websocket)
    try:
        while True:
            # Keep connection alive; clients may send ping or filter messages
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as e:
        logger.warning(f"WebSocket client error: {e}")
        ws_manager.disconnect(websocket)


# Mount frontend static directory
frontend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "frontend"))
if os.path.exists(frontend_dir):
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")


@app.get("/")
def serve_index():
    index_path = os.path.join(frontend_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "Frontend static file index.html not found"}
