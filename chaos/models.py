from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field

__all__ = [
    "IncidentType",
    "IncidentSeverity",
    "IncidentEvent",
]


class IncidentType(str, Enum):
    """
    All supported incident types the AI agent knows how to handle.
    Using Enum prevents magic strings — if you typo an incident type,
    Python raises immediately instead of silently publishing garbage to Kafka.
    """
    POD_CRASH = "POD_CRASH"
    POD_CRASH_LOOP = "POD_CRASH_LOOP"
    REDIS_OOM = "REDIS_OOM"
    QUEUE_BACKLOG = "QUEUE_BACKLOG"
    DB_CONNECTION_EXHAUSTED = "DB_CONNECTION_EXHAUSTED"
    HIGH_CPU = "HIGH_CPU"
    HIGH_MEMORY = "HIGH_MEMORY"
    DEPLOYMENT_ROLLOUT_FAILURE = "DEPLOYMENT_ROLLOUT_FAILURE"


class IncidentSeverity(str, Enum):
    """
    Severity levels for incident triage.
    CRITICAL services (payment, auth) get immediate escalation alongside remediation.
    HIGH gets 3 retry attempts.
    MEDIUM gets 2 retry attempts.
    LOW gets 1 attempt, then escalates.
    """
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class IncidentEvent(BaseModel):
    """
    Structured incident event published to Kafka incidents topic.
    This is the contract between chaos scripts and the AI agent.
    Every field the agent needs to start investigation is here.
    """

    incident_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique incident identifier for tracing across all logs"
    )
    incident_type: IncidentType = Field(
        ...,
        description="Type of failure — determines which agent path is taken"
    )
    severity: IncidentSeverity = Field(
        ...,
        description="Severity level — affects retry count and escalation logic"
    )
    service_name: str = Field(
        ...,
        description="Name of the affected Kubernetes deployment"
    )
    namespace: str = Field(
        default="autoops",
        description="Kubernetes namespace of the affected service"
    )
    description: str = Field(
        ...,
        description="Human-readable description of what happened"
    )
    metadata: dict = Field(
        default_factory=dict,
        description="Additional context: pod names, memory usage %, queue lag, etc."
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO 8601 UTC timestamp when the incident was detected"
    )
    simulated: bool = Field(
        default=True,
        description="Always True for chaos-injected events. False for real production events."
    )