from __future__ import annotations

from pydantic import BaseModel, Field, field_validator
import re

__all__ = [
    "PodInfo",
    "PodStatusResult",
    "DeploymentActionResult",
    "DeploymentInfo",
    "DeploymentCondition",
    "DeploymentSummary",
]

# ── Validators ─────────────────────────────────────────────────────────────────

DEPLOYMENT_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9\-]{0,62}[a-z0-9]$")


def validate_deployment_name(name: str) -> str:
    """
    Validates deployment name against Kubernetes naming rules.
    Prevents subprocess injection via malicious deployment names.
    """
    if not name or not DEPLOYMENT_NAME_PATTERN.match(name):
        raise ValueError(
            f"Invalid deployment name: '{name}'. "
            "Must be lowercase alphanumeric and hyphens only, 2-64 chars."
        )
    return name


# ── Pod Models ─────────────────────────────────────────────────────────────────

class PodInfo(BaseModel):
    name: str = Field(..., description="Pod name")
    phase: str = Field(..., description="Pod phase: Running, Pending, Failed, etc.")
    ready: bool = Field(..., description="Whether all containers are ready")
    restarts: int = Field(..., ge=0, description="Total restart count across all containers")


class PodStatusResult(BaseModel):
    deployment: str = Field(..., description="Deployment name")
    total: int = Field(..., ge=0, description="Total pod count")
    running: int = Field(..., ge=0, description="Running pod count")
    ready: int = Field(..., ge=0, description="Ready pod count")
    restarts: int = Field(..., ge=0, description="Total restarts across all pods")
    pods: list[PodInfo] = Field(default_factory=list, description="Individual pod details")
    healthy: bool = Field(..., description="True if all pods are running and ready")


# ── Deployment Action Models ───────────────────────────────────────────────────

class DeploymentActionResult(BaseModel):
    success: bool = Field(..., description="Whether the action succeeded")
    deployment: str = Field(..., description="Target deployment name")
    action: str = Field(..., description="Action performed: restart, scale, rollback, delete_pod")
    message: str = Field(..., description="Human readable result message")
    previous_replicas: int | None = Field(None, description="Replica count before scale action")
    new_replicas: int | None = Field(None, description="Replica count after scale action")


# ── Deployment Info Models ─────────────────────────────────────────────────────

class DeploymentCondition(BaseModel):
    type: str = Field(..., description="Condition type: Available, Progressing, etc.")
    status: str = Field(..., description="True, False, or Unknown")
    reason: str | None = Field(None, description="Machine-readable reason")
    message: str | None = Field(None, description="Human-readable message")


class DeploymentInfo(BaseModel):
    name: str = Field(..., description="Deployment name")
    desired_replicas: int = Field(..., ge=0)
    ready_replicas: int = Field(..., ge=0)
    available_replicas: int = Field(..., ge=0)
    image: str = Field(..., description="Container image currently running")
    conditions: list[DeploymentCondition] = Field(default_factory=list)
    is_healthy: bool = Field(..., description="True if ready == desired replicas")


class DeploymentSummary(BaseModel):
    name: str
    desired: int = Field(..., ge=0)
    ready: int = Field(..., ge=0)
    available: int = Field(..., ge=0)
    is_healthy: bool