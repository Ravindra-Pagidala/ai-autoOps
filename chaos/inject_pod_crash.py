from __future__ import annotations

import sys
import os
import time
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agent"))

import structlog
from config.settings import get_settings
from chaos.models import IncidentEvent, IncidentSeverity, IncidentType
from chaos.kafka_publisher import publish_incident, close_producer
from tools.kubectl_tools import get_pod_status, delete_pod

__all__ = ["inject_pod_crash", "inject_pod_crash_loop"]

logger = structlog.get_logger(__name__)


def inject_pod_crash(
    deployment_name: str = "ticket-service",
    severity: IncidentSeverity = IncidentSeverity.HIGH,
    pod_index: int = 0,
) -> IncidentEvent:
    """
    Kills one pod in the deployment by force-deleting it.
    K8s will recreate it automatically — the AI agent's job is to
    detect this, confirm recovery, and log the incident.

    pod_index: which pod to kill (0 = first, 1 = second, etc.)
    This allows parameterization — kill different pods for different scenarios.
    """
    log = logger.bind(deployment=deployment_name, severity=severity, pod_index=pod_index)
    log.info("inject_pod_crash_start")

    status = get_pod_status(deployment_name)

    if not status.pods:
        log.error("inject_pod_crash_failed", reason="no pods found")
        raise RuntimeError(f"No pods found for deployment {deployment_name}")

    target_pod = status.pods[pod_index % len(status.pods)]
    log.info("inject_pod_crash_target", pod=target_pod.name)

    delete_pod(target_pod.name)
    log.info("inject_pod_crash_pod_deleted", pod=target_pod.name)

    event = IncidentEvent(
        incident_type=IncidentType.POD_CRASH,
        severity=severity,
        service_name=deployment_name,
        description=(
            f"Pod {target_pod.name} was force-deleted. "
            f"Expected {status.total} replicas, one is now recreating."
        ),
        metadata={
            "deleted_pod": target_pod.name,
            "total_replicas": status.total,
            "pod_index": pod_index,
        },
    )

    publish_incident(event)
    log.info("inject_pod_crash_complete", incident_id=event.incident_id)
    return event


def inject_pod_crash_loop(
    deployment_name: str = "ticket-service",
    severity: IncidentSeverity = IncidentSeverity.CRITICAL,
    crash_count: int = 3,
    interval_seconds: float = 2.0,
) -> IncidentEvent:
    """
    Simulates a crash loop by repeatedly deleting pods.
    crash_count: how many pods to kill in sequence
    interval_seconds: wait between each kill

    This tests the agent's ability to detect a pattern (repeated crashes)
    vs a single crash — RCA should differ between these two scenarios.
    """
    log = logger.bind(
        deployment=deployment_name,
        severity=severity,
        crash_count=crash_count,
    )
    log.info("inject_pod_crash_loop_start")

    deleted_pods: list[str] = []

    for i in range(crash_count):
        status = get_pod_status(deployment_name)
        if not status.pods:
            log.warning("inject_pod_crash_loop_no_pods", iteration=i)
            break

        target_pod = status.pods[i % len(status.pods)]
        delete_pod(target_pod.name)
        deleted_pods.append(target_pod.name)
        log.info(
            "inject_pod_crash_loop_iteration",
            iteration=i + 1,
            total=crash_count,
            pod=target_pod.name,
        )

        if i < crash_count - 1:
            time.sleep(interval_seconds)

    event = IncidentEvent(
        incident_type=IncidentType.POD_CRASH_LOOP,
        severity=severity,
        service_name=deployment_name,
        description=(
            f"Crash loop simulated: {len(deleted_pods)} pods deleted "
            f"over {len(deleted_pods) * interval_seconds:.1f}s. "
            f"Deployment is repeatedly losing pods."
        ),
        metadata={
            "deleted_pods": deleted_pods,
            "crash_count": crash_count,
            "interval_seconds": interval_seconds,
        },
    )

    publish_incident(event)
    log.info("inject_pod_crash_loop_complete", incident_id=event.incident_id)
    return event


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inject pod crash failure")
    parser.add_argument("--deployment", default="ticket-service")
    parser.add_argument(
        "--severity",
        default="HIGH",
        choices=[s.value for s in IncidentSeverity],
    )
    parser.add_argument("--loop", action="store_true", help="Simulate crash loop")
    parser.add_argument("--crash-count", type=int, default=3)
    parser.add_argument("--pod-index", type=int, default=0)
    args = parser.parse_args()

    try:
        severity = IncidentSeverity(args.severity)
        if args.loop:
            event = inject_pod_crash_loop(
                deployment_name=args.deployment,
                severity=severity,
                crash_count=args.crash_count,
            )
        else:
            event = inject_pod_crash(
                deployment_name=args.deployment,
                severity=severity,
                pod_index=args.pod_index,
            )
        print(f"Incident published: {event.incident_id}")
    finally:
        close_producer()