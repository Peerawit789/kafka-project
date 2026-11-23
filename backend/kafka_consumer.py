import json
import logging
import os
import asyncio
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from database import SessionLocal
from models import JobModel
from websocket_manager import ws_manager

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RESULTS = os.getenv("KAFKA_TOPIC_RESULTS", "router-results")
KAFKA_GROUP_ID = os.getenv("KAFKA_BACKEND_GROUP_ID", "backend-result-processors")


def parse_iso_datetime(dt_str: str):
    if not dt_str:
        return None
    try:
        # Handles 2026-10-05T10:00:01Z or offset
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def update_job_in_db(result_data: dict) -> dict:
    job_id = result_data.get("job_id")
    if not job_id:
        logger.warning("Received result message without job_id")
        return None

    db = SessionLocal()
    try:
        job = db.query(JobModel).filter(JobModel.job_id == job_id).first()
        if not job:
            logger.warning(f"Job {job_id} not found in database. Creating placeholder.")
            job = JobModel(
                job_id=job_id,
                router_id=result_data.get("router_id", "unknown"),
                action="unknown",
            )
            db.add(job)

        status = result_data.get("status")
        if status:
            job.status = status

        if "output" in result_data and result_data["output"] is not None:
            job.output = result_data["output"]

        if "error" in result_data and result_data["error"] is not None:
            job.error = result_data["error"]

        if "attempts" in result_data and result_data["attempts"] is not None:
            job.attempts = result_data["attempts"]

        if result_data.get("started_at"):
            dt = parse_iso_datetime(result_data["started_at"])
            if dt:
                job.started_at = dt

        if result_data.get("finished_at"):
            dt = parse_iso_datetime(result_data["finished_at"])
            if dt:
                job.finished_at = dt

        db.commit()
        db.refresh(job)
        return job.to_dict()
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating job {job_id} in database: {e}")
        return None
    finally:
        db.close()


async def start_results_consumer():
    """
    Background loop that consumes router-results, updates DB, and notifies WebSocket clients.
    """
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
    }

    consumer = None
    logger.info(f"Connecting results consumer to {KAFKA_BOOTSTRAP_SERVERS} on topic {KAFKA_TOPIC_RESULTS}...")

    while True:
        try:
            if not consumer:
                consumer = Consumer(conf)
                consumer.subscribe([KAFKA_TOPIC_RESULTS])
                logger.info(f"Results consumer subscribed to {KAFKA_TOPIC_RESULTS}")

            # Run blocking poll in thread pool to not block asyncio event loop
            msg = await asyncio.to_thread(consumer.poll, 1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error on {KAFKA_TOPIC_RESULTS}: {msg.error()}")
                    continue

            val_str = msg.value().decode("utf-8")
            result_data = json.loads(val_str)
            logger.info(f"Received result for job {result_data.get('job_id')} with status {result_data.get('status')}")

            # Update DB and broadcast
            updated_job = await asyncio.to_thread(update_job_in_db, result_data)
            if updated_job:
                await ws_manager.broadcast({
                    "event": "job_updated",
                    "data": updated_job
                })

        except asyncio.CancelledError:
            logger.info("Kafka results consumer task cancelled.")
            break
        except Exception as e:
            logger.error(f"Error in Kafka results consumer: {e}. Retrying in 3 seconds...")
            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass
                consumer = None
            await asyncio.sleep(3)

    if consumer:
        try:
            consumer.close()
        except Exception:
            pass
