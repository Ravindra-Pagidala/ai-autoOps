from __future__ import annotations

import sys
import os
import argparse
import subprocess
import time
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agent"))

import structlog
from config.settings import get_settings
from chaos.models import IncidentEvent, IncidentSeverity, IncidentType
from chaos.kafka_publisher import publish_incident, close_producer

__all__ = ["inject_db_saturation"]

logger = structlog.get_logger(__name__)

_active_connections: list = []
_stop_event = threading.Event()


def inject_db_saturation(
    severity: IncidentSeverity = IncidentSeverity.HIGH,
    connection_count: int = 18,
    hold_seconds: int = 30,
    local_port: int = 5433,
) -> IncidentEvent:
    """
    Opens and holds DB connections to exhaust the connection pool.
    ticket-service Postgres has max_connections default of 100.
    We open connection_count connections and hold them for hold_seconds.

    During this window, new connection attempts from the app will fail
    with "too many connections" — simulating real DB saturation.

    connection_count: how many connections to open (keep below max_connections)
    hold_seconds: how long to hold them before releasing
    local_port: local port for kubectl port-forward to cluster postgres
    """
    settings = get_settings()
    log = logger.bind(
        connection_count=connection_count,
        hold_seconds=hold_seconds,
        severity=severity,
    )
    log.info("inject_db_saturation_start")

    log.info("inject_db_saturation_port_forward_start", local_port=local_port)
    port_forward_proc = subprocess.Popen(
        [
            "kubectl", "port-forward",
            "-n", settings.k8s_namespace,
            "deployment/postgres",
            f"{local_port}:5432",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(2)
    log.info("inject_db_saturation_port_forward_ready")

    try:
        import psycopg2

        for i in range(connection_count):
            try:
                conn = psycopg2.connect(
                    host="localhost",
                    port=local_port,
                    database="ticket_db",
                    user="autoops",
                    password="autoops_secret",
                    connect_timeout=5,
                )
                conn.autocommit = False
                _active_connections.append(conn)
                log.info(
                    "inject_db_saturation_connection_opened",
                    connection=i + 1,
                    total=connection_count,
                )
            except psycopg2.OperationalError as e:
                log.warning(
                    "inject_db_saturation_connection_failed",
                    connection=i + 1,
                    error=str(e),
                )
                break

        actual_count = len(_active_connections)
        log.info(
            "inject_db_saturation_connections_held",
            actual_count=actual_count,
            holding_for_seconds=hold_seconds,
        )

        event = IncidentEvent(
            incident_type=IncidentType.DB_CONNECTION_EXHAUSTED,
            severity=severity,
            service_name="postgres",
            description=(
                f"DB connection pool saturated: {actual_count} connections held open. "
                f"New connection attempts from ticket-service will fail. "
                f"Will release after {hold_seconds}s."
            ),
            metadata={
                "connections_held": actual_count,
                "hold_seconds": hold_seconds,
                "target_service": "postgres",
            },
        )

        publish_incident(event)
        log.info(
            "inject_db_saturation_event_published",
            incident_id=event.incident_id,
        )

        log.info(
            "inject_db_saturation_holding",
            seconds=hold_seconds,
        )
        time.sleep(hold_seconds)

        return event

    finally:
        for conn in _active_connections:
            try:
                conn.close()
            except Exception:
                pass
        _active_connections.clear()
        log.info("inject_db_saturation_connections_released")

        if port_forward_proc.poll() is None:
            port_forward_proc.terminate()
            port_forward_proc.wait(timeout=5)
        log.info("inject_db_saturation_port_forward_stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inject DB connection saturation")
    parser.add_argument(
        "--severity",
        default="HIGH",
        choices=[s.value for s in IncidentSeverity],
    )
    parser.add_argument("--connections", type=int, default=18)
    parser.add_argument("--hold", type=int, default=30)
    args = parser.parse_args()

    try:
        event = inject_db_saturation(
            severity=IncidentSeverity(args.severity),
            connection_count=args.connections,
            hold_seconds=args.hold,
        )
        print(f"Incident published: {event.incident_id}")
    finally:
        close_producer()