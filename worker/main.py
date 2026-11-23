import json
import logging
import os
import sys
import time
from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaError
from dotenv import load_dotenv

load_dotenv()

from config import get_router_info
from actions import build_cisco_commands
from ssh_client import CiscoSSHClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [worker] %(message)s"
)
logger = logging.getLogger("worker")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_COMMANDS = os.getenv("KAFKA_TOPIC_COMMANDS", "router-commands")
TOPIC_RESULTS = os.getenv("KAFKA_TOPIC_RESULTS", "router-results")
TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "router-dlq")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "router-workers")
MAX_RETRIES = int(os.getenv("SSH_MAX_RETRIES", "3"))

# Keep set of recently completed job_ids for idempotency in at-least-once delivery
COMPLETED_JOBS = set()


def get_iso_now():
    return datetime.utcnow().isoformat() + "Z"


class RouterWorker:
    def __init__(self):
        self.producer = None
        self.consumer = None
        self._init_kafka()

    def _init_kafka(self):
        logger.info(f"Connecting to Kafka brokers at: {KAFKA_BOOTSTRAP_SERVERS}")
        # Producer configuration
        prod_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'router-worker-producer',
            'acks': 'all',
            'retries': 3,
        }
        self.producer = Producer(prod_conf)

        # Consumer configuration - manual commit disabled auto-commit
        cons_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': GROUP_ID,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,
        }
        self.consumer = Consumer(cons_conf)
        self.consumer.subscribe([TOPIC_COMMANDS])
        logger.info(f"Subscribed to topic '{TOPIC_COMMANDS}' with group '{GROUP_ID}'")

    def publish_result(self, result_payload: dict):
        """Publish status/result to router-results topic with router_id as message key."""
        router_id = result_payload.get("router_id", "default")
        payload = json.dumps(result_payload).encode("utf-8")
        key = router_id.encode("utf-8")

        self.producer.produce(
            topic=TOPIC_RESULTS,
            key=key,
            value=payload
        )
        self.producer.flush()
        logger.info(f"Published result to '{TOPIC_RESULTS}' for job {result_payload.get('job_id')} (status: {result_payload.get('status')})")

    def publish_dlq(self, dlq_payload: dict):
        """Publish failed job to dead letter queue (router-dlq)."""
        router_id = dlq_payload.get("router_id", "default")
        payload = json.dumps(dlq_payload).encode("utf-8")
        key = router_id.encode("utf-8")

        self.producer.produce(
            topic=TOPIC_DLQ,
            key=key,
            value=payload
        )
        self.producer.flush()
        logger.warning(f"Sent job {dlq_payload.get('job_id')} to DLQ topic '{TOPIC_DLQ}'")

    def process_command(self, cmd_data: dict):
        job_id = cmd_data.get("job_id")
        router_id = cmd_data.get("router_id")
        action = cmd_data.get("action")
        params = cmd_data.get("params", {})

        logger.info(f"Starting execution of job {job_id} on router '{router_id}' (action: {action})")

        # Idempotency check: avoid re-running recently completed job
        if job_id in COMPLETED_JOBS:
            logger.info(f"Job {job_id} already executed. Skipping redundant run.")
            return

        started_at = get_iso_now()

        # Emit running status
        self.publish_result({
            "job_id": job_id,
            "router_id": router_id,
            "status": "running",
            "output": None,
            "error": None,
            "attempts": 0,
            "started_at": started_at,
            "finished_at": None,
        })

        # 1. Resolve router connection details
        router_info = get_router_info(router_id)
        if not router_info:
            err = f"Router '{router_id}' not found in worker inventory (routers.yaml)"
            logger.error(err)
            finished_at = get_iso_now()
            self.publish_result({
                "job_id": job_id,
                "router_id": router_id,
                "status": "failed",
                "output": None,
                "error": err,
                "attempts": 1,
                "started_at": started_at,
                "finished_at": finished_at
            })
            self.publish_dlq({
                "job_id": job_id,
                "router_id": router_id,
                "action": action,
                "params": params,
                "error": err,
                "attempts": 1,
                "failed_at": finished_at
            })
            COMPLETED_JOBS.add(job_id)
            return

        # 2. Build Cisco CLI commands
        try:
            cisco_cmds, is_config = build_cisco_commands(action, params)
        except Exception as e:
            err = f"Action translation error: {str(e)}"
            logger.error(err)
            finished_at = get_iso_now()
            self.publish_result({
                "job_id": job_id,
                "router_id": router_id,
                "status": "failed",
                "output": None,
                "error": err,
                "attempts": 1,
                "started_at": started_at,
                "finished_at": finished_at
            })
            self.publish_dlq({
                "job_id": job_id,
                "router_id": router_id,
                "action": action,
                "params": params,
                "error": err,
                "attempts": 1,
                "failed_at": finished_at
            })
            COMPLETED_JOBS.add(job_id)
            return

        # 3. Execute with retries & exponential backoff
        attempts = 0
        success = False
        last_error = None
        command_output = ""

        ssh = CiscoSSHClient(
            host=router_info["host"],
            port=router_info["port"],
            username=router_info["username"],
            password=router_info["password"],
            enable_password=router_info["enable_password"]
        )

        for attempt in range(1, MAX_RETRIES + 1):
            attempts = attempt
            logger.info(f"Connecting to router '{router_id}' ({router_info['host']}) - Attempt {attempt}/{MAX_RETRIES}...")
            try:
                ssh.connect()
                command_output = ssh.execute_commands(cisco_cmds)
                ssh.close()
                success = True
                logger.info(f"Successfully executed commands on '{router_id}' for job {job_id}")
                break
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Attempt {attempt} failed on router '{router_id}': {last_error}")
                ssh.close()
                if attempt < MAX_RETRIES:
                    backoff_delay = 2 ** (attempt - 1)
                    logger.info(f"Retrying in {backoff_delay} seconds...")
                    time.sleep(backoff_delay)

        finished_at = get_iso_now()

        if success:
            self.publish_result({
                "job_id": job_id,
                "router_id": router_id,
                "status": "success",
                "output": command_output,
                "error": None,
                "attempts": attempts,
                "started_at": started_at,
                "finished_at": finished_at
            })
        else:
            # All retries failed
            final_err_msg = f"SSH execution failed after {attempts} attempts. Last error: {last_error}"
            logger.error(f"Job {job_id} failed permanently: {final_err_msg}")

            # Send to DLQ
            self.publish_dlq({
                "job_id": job_id,
                "router_id": router_id,
                "action": action,
                "params": params,
                "error": final_err_msg,
                "attempts": attempts,
                "failed_at": finished_at
            })

            # Send failed result
            self.publish_result({
                "job_id": job_id,
                "router_id": router_id,
                "status": "failed",
                "output": None,
                "error": final_err_msg,
                "attempts": attempts,
                "started_at": started_at,
                "finished_at": finished_at
            })

        COMPLETED_JOBS.add(job_id)

    def run(self):
        logger.info("Worker started listening for commands. Press Ctrl+C to stop.")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka consumer error: {msg.error()}")
                        continue

                try:
                    payload_str = msg.value().decode("utf-8")
                    cmd_data = json.loads(payload_str)
                    logger.info(f"Received command message from {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")
                    self.process_command(cmd_data)

                    # Manual offset commit strictly AFTER processing and result publication
                    self.consumer.commit(message=msg, asynchronous=False)
                    logger.info(f"Offset committed for job {cmd_data.get('job_id')}")

                except Exception as ex:
                    logger.error(f"Unexpected error processing message: {ex}", exc_info=True)

        except KeyboardInterrupt:
            logger.info("Worker stopped by user.")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.flush()


if __name__ == "__main__":
    worker = RouterWorker()
    worker.run()
