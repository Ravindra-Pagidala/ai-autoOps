from __future__ import annotations

import sys
import os
import argparse
import subprocess
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agent"))

import structlog
from config.settings import get_settings
from chaos.models import IncidentEvent, IncidentSeverity, IncidentType
from chaos.kafka_publisher import publish_incident, close_producer
from exceptions.kubectl_exceptions import KubectlError

__all__ = ["inject_redis_oom", "restore_redis"]

logger = structlog.get_logger(__name__)

_PORT_FORWARD_PROC: subprocess.Popen | None = None


def _start_port_forward(local_port: int = 6380) -> subprocess.Popen:
    """
    Port-forwards cluster Redis to localhost so we can connect to it.
    Uses a different local port (6380) to avoid conflict with
    docker-compose Redis running on 6379.
    """
    settings = get_settings()
    log = logger.bind(local_port=local_port, namespace=settings.k8s_namespace)
    log.info("redis_port_forward_start")

    proc = subprocess.Popen(
        [
            "kubectl", "port-forward",
            "-n", settings.k8s_namespace,
            "deployment/redis",
            f"{local_port}:6379",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(2)
    log.info("redis_port_forward_ready", pid=proc.pid)
    return proc


def _stop_port_forward(proc: subprocess.Popen) -> None:
    """Terminates the port-forward process cleanly."""
    if proc and proc.poll() is None:
        proc.terminate()
        proc.wait(timeout=5)
        logger.info("redis_port_forward_stopped", pid=proc.pid)


def inject_redis_oom(
    severity: IncidentSeverity = IncidentSeverity.HIGH,
    fill_mb: int = 50,
    key_prefix: str = "chaos:oom:",
    local_port: int = 6380,
) -> IncidentEvent:
    """
    Floods cluster Redis with junk keys to simulate memory pressure.
    fill_mb: approximate MB to fill (each key ~1KB, so fill_mb * 1000 keys)
    key_prefix: prefix for all junk keys — makes cleanup easy and safe

    We only flood keys with our prefix — never touching real app data.
    Cleanup via restore_redis() deletes only chaos:oom:* keys.
    """
    global _PORT_FORWARD_PROC

    log = logger.bind(fill_mb=fill_mb, key_prefix=key_prefix, severity=severity)
    log.info("inject_redis_oom_start")

    _PORT_FORWARD_PROC = _start_port_forward(local_port)

    try:
        import redis as redis_lib
        client = redis_lib.Redis(host="localhost", port=local_port, decode_responses=True)
        client.ping()
        log.info("inject_redis_oom_connected")

        key_count = fill_mb * 1000
        junk_value = "x" * 1024

        pipe = client.pipeline(transaction=False)
        for i in range(key_count):
            pipe.set(f"{key_prefix}{i}", junk_value, ex=3600)
            if i % 500 == 0:
                pipe.execute()
                pipe = client.pipeline(transaction=False)
                log.info("inject_redis_oom_progress", keys_written=i, total=key_count)

        pipe.execute()

        info = client.info("memory")
        used_mb = info["used_memory"] / (1024 * 1024)
        log.info("inject_redis_oom_complete", used_memory_mb=round(used_mb, 2))

        event = IncidentEvent(
            incident_type=IncidentType.REDIS_OOM,
            severity=severity,
            service_name="redis",
            description=(
                f"Redis memory flooded with {key_count} junk keys (~{fill_mb}MB). "
                f"Current usage: {used_mb:.1f}MB. "
                f"Application cache operations may start failing."
            ),
            metadata={
                "keys_written": key_count,
                "fill_mb": fill_mb,
                "used_memory_mb": round(used_mb, 2),
                "key_prefix": key_prefix,
            },
        )

        publish_incident(event)
        log.info("inject_redis_oom_event_published", incident_id=event.incident_id)
        return event

    finally:
        _stop_port_forward(_PORT_FORWARD_PROC)
        _PORT_FORWARD_PROC = None


def restore_redis(
    key_prefix: str = "chaos:oom:",
    local_port: int = 6380,
) -> int:
    """
    Removes all chaos-injected keys from cluster Redis.
    Only deletes keys with our prefix — safe to run at any time.
    Returns count of deleted keys.
    """
    log = logger.bind(key_prefix=key_prefix)
    log.info("restore_redis_start")

    proc = _start_port_forward(local_port)
    try:
        import redis as redis_lib
        client = redis_lib.Redis(host="localhost", port=local_port, decode_responses=True)

        cursor = 0
        deleted = 0
        while True:
            cursor, keys = client.scan(cursor, match=f"{key_prefix}*", count=500)
            if keys:
                client.delete(*keys)
                deleted += len(keys)
            if cursor == 0:
                break

        log.info("restore_redis_complete", deleted_keys=deleted)
        return deleted
    finally:
        _stop_port_forward(proc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inject Redis OOM failure")
    parser.add_argument(
        "--severity",
        default="HIGH",
        choices=[s.value for s in IncidentSeverity],
    )
    parser.add_argument("--fill-mb", type=int, default=50)
    parser.add_argument("--restore", action="store_true", help="Restore Redis instead")
    args = parser.parse_args()

    try:
        if args.restore:
            deleted = restore_redis()
            print(f"Restored: deleted {deleted} chaos keys")
        else:
            event = inject_redis_oom(
                severity=IncidentSeverity(args.severity),
                fill_mb=args.fill_mb,
            )
            print(f"Incident published: {event.incident_id}")
    finally:
        close_producer()