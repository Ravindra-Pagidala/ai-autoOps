from __future__ import annotations

import json
import subprocess

import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.settings import get_settings
from exceptions.kubectl_exceptions import KubectlError, KubectlTimeoutError
from models.kubectl_models import (
    DeploymentActionResult,
    DeploymentCondition,
    DeploymentInfo,
    DeploymentSummary,
    PodInfo,
    PodStatusResult,
)
from utils.validators import validate_deployment_name, validate_replica_count

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


def _get_namespace() -> str:
    return get_settings().k8s_namespace


def _get_timeout() -> int:
    return get_settings().kubectl_timeout_seconds


# ── Core Executor ──────────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type(KubectlTimeoutError),
    reraise=True,
)
def _run_kubectl(args: list[str], timeout: int | None = None) -> str:
    """
    Core kubectl executor. Every kubectl call in this module goes through here.

    Security: uses list args with shell=False — prevents shell injection.
    Reliability: retries on timeout (transient), not on KubectlError (permanent).
    Observability: logs command, result, and errors with full context.
    """
    effective_timeout = timeout or _get_timeout()
    command = ["kubectl"] + args
    log = logger.bind(command=" ".join(command), timeout=effective_timeout)
    log.info("kubectl_executing")

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=effective_timeout,
            shell=False,
        )
    except subprocess.TimeoutExpired:
        log.error("kubectl_timeout", timeout_seconds=effective_timeout)
        raise KubectlTimeoutError(
            message=f"kubectl timed out after {effective_timeout}s",
            command=" ".join(command),
            timeout_seconds=effective_timeout,
        )
    except FileNotFoundError:
        log.error("kubectl_not_found")
        raise KubectlError(
            message="kubectl binary not found. Is it installed and in PATH?",
            command=" ".join(command),
        )

    if result.returncode != 0:
        log.error(
            "kubectl_failed",
            returncode=result.returncode,
            stderr=result.stderr.strip(),
        )
        raise KubectlError(
            message=f"kubectl failed (exit {result.returncode}): {result.stderr.strip()}",
            command=" ".join(command),
            returncode=result.returncode,
        )

    log.info("kubectl_succeeded", output_preview=result.stdout.strip()[:150])
    return result.stdout.strip()


# ── Pod Status ─────────────────────────────────────────────────────────────────

def get_pod_status(deployment_name: str) -> PodStatusResult:
    """
    Returns current pod health for a deployment.
    Used by validator agent after remediation to confirm recovery.
    """
    validated_name = validate_deployment_name(deployment_name)
    namespace = _get_namespace()
    log = logger.bind(deployment=validated_name, namespace=namespace)
    log.info("get_pod_status_start")

    output = _run_kubectl([
        "get", "pods",
        "-n", namespace,
        "-l", f"app={validated_name}",
        "-o", "json",
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
        is_ready = bool(container_statuses) and all(
            cs.get("ready", False) for cs in container_statuses
        )
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
            restarts=restart_count,
        ))

    result = PodStatusResult(
        deployment=validated_name,
        total=len(pods_raw),
        running=running_count,
        ready=ready_count,
        restarts=total_restarts,
        pods=pod_infos,
        healthy=(len(pods_raw) > 0 and ready_count == len(pods_raw)),
    )

    log.info(
        "get_pod_status_complete",
        total=result.total,
        running=result.running,
        ready=result.ready,
        healthy=result.healthy,
    )
    return result


# ── Restart Deployment ─────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(KubectlError),
    reraise=True,
)
def restart_deployment(deployment_name: str) -> DeploymentActionResult:
    """
    Performs a rolling restart — replaces pods one by one, zero downtime.
    Primary remediation for pod crash loops and stuck deployments.
    Retries up to 3 times with exponential backoff.
    """
    validated_name = validate_deployment_name(deployment_name)
    namespace = _get_namespace()
    log = logger.bind(deployment=validated_name, namespace=namespace)
    log.info("restart_deployment_start")

    _run_kubectl([
        "rollout", "restart",
        f"deployment/{validated_name}",
        "-n", namespace,
    ])

    log.info("restart_deployment_success")
    return DeploymentActionResult(
        success=True,
        deployment=validated_name,
        action="restart",
        message=f"Rolling restart triggered for deployment/{validated_name}",
    )


# ── Scale Deployment ───────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(KubectlError),
    reraise=True,
)
def scale_deployment(deployment_name: str, replicas: int) -> DeploymentActionResult:
    """
    Scales a deployment to the specified replica count.
    Used to scale up under load or scale down to force pod recreation.
    Validates bounds before touching kubectl.
    Retries up to 3 times with exponential backoff.
    """
    validated_name = validate_deployment_name(deployment_name)
    validated_replicas = validate_replica_count(replicas, deployment_name)
    namespace = _get_namespace()

    current_info = get_deployment_info(validated_name)
    previous_replicas = current_info.desired_replicas

    log = logger.bind(
        deployment=validated_name,
        namespace=namespace,
        from_replicas=previous_replicas,
        to_replicas=validated_replicas,
    )
    log.info("scale_deployment_start")

    _run_kubectl([
        "scale", f"deployment/{validated_name}",
        f"--replicas={validated_replicas}",
        "-n", namespace,
    ])

    log.info("scale_deployment_success")
    return DeploymentActionResult(
        success=True,
        deployment=validated_name,
        action="scale",
        message=(
            f"Scaled deployment/{validated_name} "
            f"from {previous_replicas} to {validated_replicas} replicas"
        ),
        previous_replicas=previous_replicas,
        new_replicas=validated_replicas,
    )


# ── Rollback Deployment ────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(KubectlError),
    reraise=True,
)
def rollback_deployment(deployment_name: str) -> DeploymentActionResult:
    """
    Rolls back a deployment to its previous revision.
    Used when a bad deployment causes cascading failures.
    Retries up to 3 times with exponential backoff.
    """
    validated_name = validate_deployment_name(deployment_name)
    namespace = _get_namespace()
    log = logger.bind(deployment=validated_name, namespace=namespace)
    log.info("rollback_deployment_start")

    _run_kubectl([
        "rollout", "undo",
        f"deployment/{validated_name}",
        "-n", namespace,
    ])

    log.info("rollback_deployment_success")
    return DeploymentActionResult(
        success=True,
        deployment=validated_name,
        action="rollback",
        message=f"Rolled back deployment/{validated_name} to previous revision",
    )


# ── Delete Pod ─────────────────────────────────────────────────────────────────

def delete_pod(pod_name: str) -> DeploymentActionResult:
    """
    Force deletes a specific pod. K8s automatically recreates it via the Deployment.
    Used for stuck or crash-looping pods where rolling restart is insufficient.
    No retry decorator — this is intentionally a one-shot operation.
    """
    validated_pod = validate_deployment_name(pod_name)
    namespace = _get_namespace()
    log = logger.bind(pod=validated_pod, namespace=namespace)
    log.info("delete_pod_start")

    _run_kubectl([
        "delete", "pod", validated_pod,
        "-n", namespace,
        "--grace-period=0",
        "--force",
    ])

    log.info("delete_pod_success")
    return DeploymentActionResult(
        success=True,
        deployment=validated_pod,
        action="delete_pod",
        message=f"Force deleted pod {validated_pod} — K8s will recreate automatically",
    )


# ── Get Deployment Info ────────────────────────────────────────────────────────

def get_deployment_info(deployment_name: str) -> DeploymentInfo:
    """
    Returns full deployment info: replicas, image, health conditions.
    Called by RCA agent to assess current state before deciding on remediation.
    Also called internally by scale_deployment to capture previous replica count.
    """
    validated_name = validate_deployment_name(deployment_name)
    namespace = _get_namespace()
    log = logger.bind(deployment=validated_name, namespace=namespace)
    log.info("get_deployment_info_start")

    output = _run_kubectl([
        "get", f"deployment/{validated_name}",
        "-n", namespace,
        "-o", "json",
    ])

    data = json.loads(output)
    spec = data.get("spec", {})
    status = data.get("status", {})

    desired_replicas: int = spec.get("replicas", 0) or 0
    ready_replicas: int = status.get("readyReplicas", 0) or 0
    available_replicas: int = status.get("availableReplicas", 0) or 0

    conditions = [
        DeploymentCondition(
            type=c.get("type", "Unknown"),
            status=c.get("status", "Unknown"),
            reason=c.get("reason"),
            message=c.get("message"),
        )
        for c in status.get("conditions", [])
    ]

    result = DeploymentInfo(
        name=validated_name,
        desired_replicas=desired_replicas,
        ready_replicas=ready_replicas,
        available_replicas=available_replicas,
        image=(
            spec.get("template", {})
                .get("spec", {})
                .get("containers", [{}])[0]
                .get("image", "unknown")
        ),
        conditions=conditions,
        is_healthy=(desired_replicas > 0 and ready_replicas == desired_replicas),
    )

    log.info(
        "get_deployment_info_complete",
        desired=result.desired_replicas,
        ready=result.ready_replicas,
        healthy=result.is_healthy,
    )
    return result


# ── List Deployments ───────────────────────────────────────────────────────────

def list_deployments() -> list[DeploymentSummary]:
    """
    Lists all deployments in the configured namespace with health status.
    Called by supervisor agent at the start of every incident investigation
    to get a full picture of the cluster state.
    """
    namespace = _get_namespace()
    log = logger.bind(namespace=namespace)
    log.info("list_deployments_start")

    output = _run_kubectl([
        "get", "deployments",
        "-n", namespace,
        "-o", "json",
    ])

    data = json.loads(output)
    summaries: list[DeploymentSummary] = []

    for item in data.get("items", []):
        desired: int = item["spec"].get("replicas", 0) or 0
        ready: int = item["status"].get("readyReplicas", 0) or 0
        available: int = item["status"].get("availableReplicas", 0) or 0

        summaries.append(DeploymentSummary(
            name=item["metadata"]["name"],
            desired=desired,
            ready=ready,
            available=available,
            is_healthy=(desired > 0 and ready == desired),
        ))

    log.info("list_deployments_complete", count=len(summaries))
    return summaries