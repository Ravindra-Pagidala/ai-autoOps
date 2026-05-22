from __future__ import annotations

__all__ = [
    "KubectlError",
    "KubectlTimeoutError",
    "KubectlValidationError",
]


class KubectlError(Exception):
    """
    Raised when a kubectl command exits with a non-zero return code.
    This is a permanent failure — the command ran but failed.
    Retries may help for transient API issues but not for
    invalid resource names or missing deployments.
    """

    def __init__(self, message: str, command: str = "", returncode: int = -1) -> None:
        super().__init__(message)
        self.command = command
        self.returncode = returncode


class KubectlTimeoutError(KubectlError):
    """
    Raised when a kubectl command exceeds its configured timeout.
    This is a transient failure — worth retrying.
    Usually caused by K8s API server being temporarily overloaded.
    """

    def __init__(self, message: str, command: str = "", timeout_seconds: int = 0) -> None:
        super().__init__(message, command=command)
        self.timeout_seconds = timeout_seconds


class KubectlValidationError(KubectlError):
    """
    Raised when input validation fails before kubectl is even called.
    This is a permanent failure — retrying will not help.
    Caused by invalid deployment names, bad replica counts, etc.
    """
    pass