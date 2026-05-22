from __future__ import annotations

import re

from exceptions.kubectl_exceptions import KubectlValidationError

__all__ = [
    "validate_deployment_name",
    "validate_replica_count",
    "validate_namespace",
]

# Kubernetes naming rules:
# - lowercase alphanumeric and hyphens only
# - must start and end with alphanumeric
# - max 63 characters (DNS subdomain limit)
_DEPLOYMENT_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9\-]{0,61}[a-z0-9]$")
_SINGLE_CHAR_PATTERN = re.compile(r"^[a-z0-9]$")

_MAX_REPLICAS = 50
_MIN_REPLICAS = 0


def validate_deployment_name(name: str) -> str:
    """
    Validates a Kubernetes deployment or pod name.

    Enforces K8s naming rules AND prevents shell injection —
    malicious names like "ticket-service; rm -rf /" are rejected
    before they ever reach subprocess.

    Returns the validated name unchanged.
    Raises KubectlValidationError on invalid input.
    """
    if not name or not isinstance(name, str):
        raise KubectlValidationError(
            f"Deployment name must be a non-empty string, got: {type(name).__name__}"
        )

    name = name.strip()

    # Single character names are valid K8s names
    if len(name) == 1:
        if not _SINGLE_CHAR_PATTERN.match(name):
            raise KubectlValidationError(
                f"Invalid deployment name: '{name}'. Single char must be lowercase alphanumeric."
            )
        return name

    if not _DEPLOYMENT_NAME_PATTERN.match(name):
        raise KubectlValidationError(
            f"Invalid deployment name: '{name}'. "
            "Must be lowercase alphanumeric and hyphens only, "
            "start and end with alphanumeric, max 63 chars. "
            "This validation prevents shell injection attacks."
        )

    return name


def validate_replica_count(replicas: int, deployment_name: str = "") -> int:
    """
    Validates replica count is within safe operational bounds.

    Upper bound of 50 prevents accidental resource exhaustion on
    a local kind cluster. Adjust via MAX_REPLICAS env var in production.

    Returns the validated replica count unchanged.
    Raises KubectlValidationError on invalid input.
    """
    if not isinstance(replicas, int):
        raise KubectlValidationError(
            f"Replica count must be an integer, got: {type(replicas).__name__}"
        )

    if not (_MIN_REPLICAS <= replicas <= _MAX_REPLICAS):
        raise KubectlValidationError(
            f"Invalid replica count: {replicas} for deployment '{deployment_name}'. "
            f"Must be between {_MIN_REPLICAS} and {_MAX_REPLICAS}."
        )

    return replicas


def validate_namespace(namespace: str) -> str:
    """
    Validates a Kubernetes namespace name.
    Same rules as deployment names.
    """
    if not namespace or not isinstance(namespace, str):
        raise KubectlValidationError(
            f"Namespace must be a non-empty string, got: {type(namespace).__name__}"
        )

    namespace = namespace.strip()

    if not _DEPLOYMENT_NAME_PATTERN.match(namespace):
        raise KubectlValidationError(
            f"Invalid namespace: '{namespace}'. "
            "Must be lowercase alphanumeric and hyphens only."
        )

    return namespace