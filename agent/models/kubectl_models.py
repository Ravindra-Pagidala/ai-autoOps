from __future__ import annotations

from pydantic import BaseModel, Field

__all__ = [
    "PodInfo",
    "PodStatusResult",
    "DeploymentActionResult",
    "DeploymentCondition",
    "DeploymentInfo",
    "DeploymentSummary",
]


class PodInfo(BaseModel):
    """Represents the current state of a single Kubernetes pod."""

    name: str = Field(..., description="Full pod name e.g. ticket-service-6cb54867cc-22zq8")
    phase: str = Field(..., description="Pod phase: Running, Pending, Failed, Succeeded, Unknown")
    ready: bool = Field(..., description="True if all containers in the pod are ready")
    restarts: int = Field(..., ge=0, description="Total container restart count for this pod")


class PodStatusResult(BaseModel):
    """Aggregated health status for all pods in a deployment."""

    deployment: str = Field(..., description="Deployment name")
    total: int = Field(..., ge=0, description="Total pod count across all replicas")
    running: int = Field(..., ge=0, description="Count of pods in Running phase")
    ready: int = Field(..., ge=0, description="Count of pods where all containers are ready")
    restarts: int = Field(..., ge=0, description="Total restart count across all pods")
    pods: list[PodInfo] = Field(default_factory=list, description="Per-pod breakdown")
    healthy: bool = Field(
        ...,
        description="True only when total > 0 and every pod is ready. "
                    "Used by validator agent to confirm successful remediation."
    )


class DeploymentActionResult(BaseModel):
    """Result of a kubectl action: restart, scale, rollback, or delete_pod."""

    success: bool = Field(..., description="Whether the action completed without error")
    deployment: str = Field(..., description="Target deployment or pod name")
    action: str = Field(
        ...,
        description="Action performed: restart | scale | rollback | delete_pod"
    )
    message: str = Field(..., description="Human-readable description of what happened")
    previous_replicas: int | None = Field(
        None,
        description="Replica count before a scale action. None for non-scale actions."
    )
    new_replicas: int | None = Field(
        None,
        description="Replica count after a scale action. None for non-scale actions."
    )


class DeploymentCondition(BaseModel):
    """A single Kubernetes deployment condition from the status block."""

    type: str = Field(..., description="Condition type: Available, Progressing, ReplicaFailure")
    status: str = Field(..., description="True, False, or Unknown")
    reason: str | None = Field(None, description="Machine-readable reason code")
    message: str | None = Field(None, description="Human-readable explanation")


class DeploymentInfo(BaseModel):
    """Full deployment details used by the RCA agent before deciding on remediation."""

    name: str = Field(..., description="Deployment name")
    desired_replicas: int = Field(..., ge=0, description="Configured replica count")
    ready_replicas: int = Field(..., ge=0, description="Currently ready replica count")
    available_replicas: int = Field(..., ge=0, description="Currently available replica count")
    image: str = Field(..., description="Container image currently running")
    conditions: list[DeploymentCondition] = Field(
        default_factory=list,
        description="K8s deployment conditions for health assessment"
    )
    is_healthy: bool = Field(
        ...,
        description="True when ready_replicas equals desired_replicas and desired > 0"
    )


class DeploymentSummary(BaseModel):
    """Lightweight deployment overview for the supervisor agent's initial cluster scan."""

    name: str = Field(..., description="Deployment name")
    desired: int = Field(..., ge=0)
    ready: int = Field(..., ge=0)
    available: int = Field(..., ge=0)
    is_healthy: bool = Field(..., description="True when ready equals desired and desired > 0")