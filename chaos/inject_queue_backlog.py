from __future__ import annotations

import sys
import os
import argparse
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agent"))

import structlog
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config.settings import get_settings
from chaos.models import IncidentEvent, IncidentSeverity, IncidentType
from chaos.kafka_publisher import publish_incident, close_producer

__all__ = ["inject_queue_backlog"]

logger = structlog.get_logger(__name__)


def inject_queue_backlog(
    target_topic: str = "incidents",
    message_count: int = 10000,
    severity: IncidentSeverity = IncidentSeverity.HIGH,
    batch_size: int = 500,
) -> IncidentEvent:
    """
    Floods a Kafka topic with messages to simulate queue backlog.
    In production this happens when consumers are too slow or crash —
    producer keeps writing, consumer falls behind, lag grows.

    Uses a separate producer from the incident publisher to avoid
    polluting the incidents topic with noise messages.
    target_topic: topic to flood (default: incidents to trigger agent)
    message_count: total messages to publish
    batch_size: messages per batch (higher = faster but more memory)
    """
    settings = get_settings()
    log = logger.bind(
        target_topic=target_topic,
        message_count=message_count,
        severity=severity,
    )
    log.info("inject_queue_backlog_start")

    flood_producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        linger_ms=10,
        batch_size=16384,
    )

    try:
        published = 0
        junk_message = {
            "type": "CHAOS_BACKLOG",
            "source": "inject_queue_backlog",
            "index": 0,
        }

        for i in range(message_count):
            junk_message["index"] = i
            flood_producer.send(target_topic, value=junk_message)
            published += 1

            if published % batch_size == 0:
                flood_producer.flush()
                log.info(
                    "inject_queue_backlog_progress",
                    published=published,
                    total=message_count,
                )

        flood_producer.flush()
        log.info("inject_queue_backlog_complete", total_published=published)

    finally:
        flood_producer.close()

    event = IncidentEvent(
        incident_type=IncidentType.QUEUE_BACKLOG,
        severity=severity,
        service_name="kafka",
        description=(
            f"Queue backlog injected: {message_count} messages published to "
            f"topic '{target_topic}'. Consumer lag growing. "
            f"Services depending on this topic may start timing out."
        ),
        metadata={
            "target_topic": target_topic,
            "message_count": message_count,
            "batch_size": batch_size,
        },
    )

    publish_incident(event)
    log.info("inject_queue_backlog_event_published", incident_id=event.incident_id)
    return event


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inject Kafka queue backlog")
    parser.add_argument("--topic", default="incidents")
    parser.add_argument("--count", type=int, default=10000)
    parser.add_argument(
        "--severity",
        default="HIGH",
        choices=[s.value for s in IncidentSeverity],
    )
    args = parser.parse_args()

    try:
        event = inject_queue_backlog(
            target_topic=args.topic,
            message_count=args.count,
            severity=IncidentSeverity(args.severity),
        )
        print(f"Incident published: {event.incident_id}")
    finally:
        close_producer()