from __future__ import annotations

import json
import sys
import os

# Add agent/ to path so we can use config and structlog from the venv
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agent"))

import structlog
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.settings import get_settings
from chaos.models import IncidentEvent

__all__ = ["publish_incident", "get_producer"]

logger = structlog.get_logger(__name__)

_producer: KafkaProducer | None = None


def get_producer() -> KafkaProducer:
    """
    Returns singleton KafkaProducer.
    Created once and reused — creating a new producer per message is expensive.
    Like a database connection pool — create once, reuse always.
    """
    global _producer
    if _producer is None:
        settings = get_settings()
        log = logger.bind(bootstrap_servers=settings.kafka_bootstrap_servers)
        log.info("kafka_producer_creating")

        _producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            max_block_ms=10000,
            request_timeout_ms=15000,
        )
        log.info("kafka_producer_created")

    return _producer


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((KafkaError, NoBrokersAvailable)),
    reraise=True,
)
def publish_incident(event: IncidentEvent) -> None:
    """
    Publishes an incident event to the Kafka incidents topic.
    Uses incident_id as the message key — ensures all events for the same
    incident go to the same partition (ordering guarantee).
    Retries up to 3 times with exponential backoff on Kafka errors.
    """
    settings = get_settings()
    topic = settings.kafka_topic_incidents
    producer = get_producer()

    log = logger.bind(
        incident_id=event.incident_id,
        incident_type=event.incident_type,
        service=event.service_name,
        severity=event.severity,
        topic=topic,
    )
    log.info("publishing_incident_event")

    future = producer.send(
        topic=topic,
        key=event.incident_id,
        value=event.model_dump(),
    )

    try:
        record_metadata = future.get(timeout=10)
        log.info(
            "incident_event_published",
            partition=record_metadata.partition,
            offset=record_metadata.offset,
        )
    except KafkaError as e:
        log.error("incident_event_publish_failed", error=str(e))
        raise


def close_producer() -> None:
    """Flushes and closes the Kafka producer. Call at script exit."""
    global _producer
    if _producer is not None:
        logger.info("kafka_producer_closing")
        _producer.flush()
        _producer.close()
        _producer = None
        logger.info("kafka_producer_closed")