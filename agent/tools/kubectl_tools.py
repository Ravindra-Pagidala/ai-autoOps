from __future__ import annotations

import json
import subprocess
from typing import Optional

import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from models.kubectl_models import (
    DeploymentActionResult,
    DeploymentCondition,
    DeploymentInfo,
    DeploymentSummary,
    PodInfo,
    PodStatusResult,
    validate_deployment_name,
)

__all__ = [
    "get_pod_status",
    "restart_deployment",
    "scale_deployment",
    "rollback_deployment",
    "delete_pod",
    "get_deployment_info",
    "list_deployments",
]

logger = structlog.get_logger(__name__)

NAMESPACE = "autoops"
DEFAULT_KUBECTL_TIMEOUT = 30


# ── Exceptions ─────────────────────────────────────────────────────────────────

class KubectlError(Exception):
    """Raised when a kubectl command exits with non-zero code."""
    pass


class KubectlTimeoutError(KubectlError):
    """Raised when a kubectl command exceeds its timeout."""
    pass


# ── Core Executor ──────────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type(KubectlTimeoutError),
    reraise=True
)
def _run_kubectl(args: list[str], timeout: int = DEFAULT_KUBECTL_TIMEOUT) -> str:
    """
    Core kubectl executor. Every kubectl call goes through here.

    - Uses list args (never shell=True) — prevents shell injection
    - Enforces timeout on every call
    - Retries on timeout (transient) but NOT on KubectlError (permanent failures)
    - Logs before and after every call for full observability
    """
    command = ["kubectl"] + args
    log = logger.bind(command=" ".join(command))
    log.info("kubectl_executing")

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            shell=False  # explicit — never use shell=True with user-influenced args
        )
    except subprocess.TimeoutExpired:
        log.error("kubectl_timeout", timeout_seconds=timeout)
        raise KubectlTimeoutError(
            f"kubectl timed out after {timeout}s: {' '.join(command)}"
        )

    if result.returncode != 0:
        log.error(
            "kubectl_failed",
            returncode=result.returncode,
            stderr=result.stderr.strip()
        )
        raise KubectlError(
            f"kubectl failed (exit {result.returncode}): {result.stderr.strip()}"
        )

    log.info("kubectl_succeeded", output_preview=result.stdout.strip()[:150])
    return result.stdout.strip()


# ── Pod Status ─────────────────────────────────────────────────────────────────

def get_pod_status(deployment_name: str) -> PodStatusResult:
    """
    Returns current pod health status for a deployment.
    Used by the validator agent to confirm remediation success.
    """
    validated_name = validate_deployment_name(deployment_name)
    log = logger.bind(deployment=validated_name, namespace=NAMESPACE)
    log.info("get_pod_status_start")

    output = _run_kubectl([
        "get", "pods",
        "-n", NAMESPACE,
        "-l", f"app={validated_name}",
        "-o", "json"
    ])

    data = json.loads(output)
    pods_raw = data.get("items", [])

    pod_infos: list[PodInfo] = []
    total_restarts = 0
    running_count = 0
    ready_count = 0

    for pod in pods_raw:
        phase = pod["status"].get("phase", "Unknown")
        container_statuses = pod["status"].get("containerStatuses", [])
        is_ready = all(cs.get("ready", False) for cs in container_statuses)
        restart_count = sum(cs.get("restartCount", 0) for cs in container_statuses)

        if phase == "Running":
            running_count += 1
        if is_ready:
            ready_count += 1

        total_restarts += restart_count
        pod_infos.append(PodInfo(
            name=pod["metadata"]["name"],
            phase=phase,
            ready=is_ready,
            restarts=restart_count
        ))

    result = PodStatusResult(
        deployment=validated_name,
        total=len(pods_raw),
        running=running_count,
        ready=ready_count,
        restarts=total_restarts,
        pods=pod_infos,
        healthy=(len(pods_raw) > 0 and ready_count == len(pods_raw))
    )

    log.info(
        "get_pod_status_complete",
        total=result.total,
        running=result.running,
        ready=result.ready,
        healthy=result.healthy
    )
    return result


# ── Restart Deployment ─────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(KubectlError),
    reraise=True
)
def restart_deployment(deployment_name: str) -> DeploymentActionResult:
    """
    Performs a rolling restart of a deployment.
    K8s replaces pods one by one — zero downtime.
    Retries up to 3 times with exponential backoff on failure.
    """
    validated_name = validate_deployment_name(deployment_name)
    log = logger.bind(deployment=validated_name, namespace=NAMESPACE)
    log.info("restart_deployment_start")

    _run_kubectl([
        "rollout", "restart",
        f"deployment/{validated_name}",
        "-n", NAMESPACE
    ])

    log.info("restart_deployment_success")
    return DeploymentActionResult(
        success=True,
        deployment=validated_name,
        action="restart",
        message=f"Rolling restart triggered for deployment/{validated_name}"
    )


# ── Scale Deployment ───────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(KubectlError),
    reraise=True
)
def scale_deployment(deployment_name: str, replicas: int) -> DeploymentActionResult:
    """
    Scales a deployment to the specified replica count.
    Validates replica bounds before touching kubectl.
    Retries up to 3 times with exponential backoff on failure.
    """
    validated_name = validate_deployment_name(deployment_name)

    if not (0 <= replicas <= 20):
        raise ValueError(
            f"Invalid replica count: {replicas}. Must be between 0 and 20."
        )

    current_info = get_deployment_info(validated_name)
    previous_replicas = current_info.desired_replicas

    log = logger.bind(
        deployment=validated_name,
        namespace=NAMESPACE,
        from_replicas=previous_replicas,
        to_replicas=replicas
    )
    log.info("scale_deployment_start")

    _run_kubectl([
        "scale", f"deployment/{validated_name}",
        f"--replicas={replicas}",
        "-n", NAMESPACE
    ])

    log.info("scale_deployment_success")
    return DeploymentActionResult(
        success=True,
        deployment=validated_name,
        action="scale",
        message=f"Scaled deployment/{validated_name} from {previous_replicas} to {replicas} replicas",
        previous_replicas=previous_replicas,
        new_replicas=replicas
    )


# ── Rollback Deployment ────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(KubectlError),
    reraise=True
)
def rollback_deployment(deployment_name: str) -> DeploymentActionResult:
    """
    Rolls back a deployment to its previous revision.
    Used when a bad deployment causes cascading failures.
    Retries up to 3 times with exponential backoff on failure.
    """
    validated_name = validate_deployment_name(deployment_name)
    log = logger.bind(deployment=validated_name, namespace=NAMESPACE)
    log.info("rollback_deployment_start")

    _run_kubectl([
        "rollout", "undo",
        f"deployment/{validated_name}",
        "-n", NAMESPACE
    ])

    log.info("rollback_deployment_success")
    return DeploymentActionResult(
        success=True,
        deployment=validated_name,
        action="rollback",
        message=f"Rolled back deployment/{validated_name} to previous revision"
    )


# ── Delete Pod ─────────────────────────────────────────────────────────────────

def delete_pod(pod_name: str) -> DeploymentActionResult:
    """
    Force deletes a specific pod. K8s automatically recreates it.
    Used for stuck or crash-looping pods that rolling restart won't fix.
    pod_name validated against same naming rules as deployment names.
    """
    validated_pod = validate_deployment_name(pod_name)
    log = logger.bind(pod=validated_pod, namespace=NAMESPACE)
    log.info("delete_pod_start")

    _run_kubectl([
        "delete", "pod", validated_pod,
        "-n", NAMESPACE,
        "--grace-period=0",
        "--force"
    ])

    log.info("delete_pod_success")
    return DeploymentActionResult(
        success=True,
        deployment=validated_pod,
        action="delete_pod",
        message=f"Force deleted pod {validated_pod} — K8s will recreate automatically"
    )


# ── Get Deployment Info ────────────────────────────────────────────────────────

def get_deployment_info(deployment_name: str) -> DeploymentInfo:
    """
    Returns full deployment info: replicas, image, health conditions.
    Used by RCA agent to understand current deployment state before acting.
    """
    validated_name = validate_deployment_name(deployment_name)
    log = logger.bind(deployment=validated_name, namespace=NAMESPACE)
    log.info("get_deployment_info_start")

    output = _run_kubectl([
        "get", f"deployment/{validated_name}",
        "-n", NAMESPACE,
        "-o", "json"
    ])

    data = json.loads(output)
    spec = data.get("spec", {})
    status = data.get("status", {})

    ready_replicas = status.get("readyReplicas", 0) or 0
    desired_replicas = spec.get("replicas", 0) or 0

    conditions = [
        DeploymentCondition(
            type=c.get("type", "Unknown"),
            status=c.get("status", "Unknown"),
            reason=c.get("reason"),
            message=c.get("message")
        )
        for c in status.get("conditions", [])
    ]

    result = DeploymentInfo(
        name=validated_name,
        desired_replicas=desired_replicas,
        ready_replicas=ready_replicas,
        available_replicas=status.get("availableReplicas", 0) or 0,
        image=(
            spec.get("template", {})
                .get("spec", {})
                .get("containers", [{}])[0]
                .get("image", "unknown")
        ),
        conditions=conditions,
        is_healthy=(desired_replicas > 0 and ready_replicas == desired_replicas)
    )

    log.info(
        "get_deployment_info_complete",
        desired=result.desired_replicas,
        ready=result.ready_replicas,
        healthy=result.is_healthy
    )
    return result


# ── List Deployments ───────────────────────────────────────────────────────────

def list_deployments() -> list[DeploymentSummary]:
    """
    Lists all deployments in the autoops namespace with health status.
    Used by supervisor agent for initial cluster state assessment.
    """
    log = logger.bind(namespace=NAMESPACE)
    log.info("list_deployments_start")

    output = _run_kubectl([
        "get", "deployments",
        "-n", NAMESPACE,
        "-o", "json"
    ])

    data = json.loads(output)
    summaries: list[DeploymentSummary] = []

    for item in data.get("items", []):
        desired = item["spec"].get("replicas", 0) or 0
        ready = item["status"].get("readyReplicas", 0) or 0
        available = item["status"].get("availableReplicas", 0) or 0

        summaries.append(DeploymentSummary(
            name=item["metadata"]["name"],
            desired=desired,
            ready=ready,
            available=available,
            is_healthy=(desired > 0 and ready == desired)
        ))

    log.info("list_deployments_complete", count=len(summaries))
    return summaries