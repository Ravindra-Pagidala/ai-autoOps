from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["Settings", "get_settings"]


class Settings(BaseSettings):
    """
    All application configuration loaded from environment variables.
    Validated on startup — app refuses to start if required vars are missing.
    Fail fast is better than failing mysteriously at runtime.

    Uses pydantic-settings which automatically reads from:
    1. Environment variables
    2. .env file (via env_file config)
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    # ── Groq LLM ──────────────────────────────────────────────────────────────
    groq_api_key: str = Field(..., description="Groq API key for LLM calls")
    groq_model: str = Field(
        default="llama-3.1-70b-versatile",
        description="Groq model name"
    )
    llm_temperature: float = Field(
        default=0.1,
        ge=0.0,
        le=2.0,
        description="LLM temperature. Low = deterministic, high = creative. Agents need low."
    )
    llm_max_tokens: int = Field(
        default=2048,
        ge=256,
        le=8192,
        description="Max tokens per LLM response"
    )

    # ── Kafka ─────────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated Kafka broker addresses"
    )
    kafka_topic_incidents: str = Field(
        default="incidents",
        description="Topic where failure events are published"
    )
    kafka_topic_remediation_results: str = Field(
        default="remediation-results",
        description="Topic where agent publishes remediation outcomes"
    )
    kafka_topic_escalations: str = Field(
        default="escalations",
        description="Topic for incidents that could not be auto-resolved"
    )
    kafka_consumer_group: str = Field(
        default="autoops-agent",
        description="Kafka consumer group ID for the AI agent"
    )
    kafka_poll_timeout_ms: int = Field(
        default=1000,
        ge=100,
        description="Kafka consumer poll timeout in milliseconds"
    )

    # ── Redis ─────────────────────────────────────────────────────────────────
    redis_host: str = Field(default="localhost")
    redis_port: int = Field(default=6379, ge=1, le=65535)
    redis_memory_ttl_seconds: int = Field(
        default=3600,
        description="TTL for active incident context in Redis. Default 1 hour."
    )

    # ── PostgreSQL ────────────────────────────────────────────────────────────
    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=5432, ge=1, le=65535)
    postgres_user: str = Field(default="autoops")
    postgres_password: str = Field(..., description="PostgreSQL password")
    postgres_db: str = Field(default="autoops_db")
    postgres_max_connections: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Max DB connections in pool"
    )

    # ── Kubernetes ────────────────────────────────────────────────────────────
    k8s_namespace: str = Field(
        default="autoops",
        description="Kubernetes namespace where monitored services run"
    )
    kubectl_timeout_seconds: int = Field(
        default=30,
        ge=5,
        le=120,
        description="Timeout for individual kubectl commands"
    )
    kubectl_max_replicas: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Maximum replicas allowed in scale operations"
    )

    # ── Agent Behaviour ───────────────────────────────────────────────────────
    max_remediation_retries: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Max times agent retries remediation before escalating"
    )
    validator_wait_seconds: int = Field(
        default=15,
        ge=5,
        le=120,
        description="Seconds to wait after remediation before validating outcome"
    )

    # ── FastAPI ───────────────────────────────────────────────────────────────
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000, ge=1024, le=65535)
    api_debug: bool = Field(default=False)

    @field_validator("groq_api_key")
    @classmethod
    def groq_api_key_must_not_be_placeholder(cls, v: str) -> str:
        if v.strip() in ("your_actual_groq_api_key_here", "changeme", ""):
            raise ValueError(
                "GROQ_API_KEY is set to a placeholder value. "
                "Set a real Groq API key in your .env file."
            )
        return v.strip()

    @field_validator("kafka_bootstrap_servers")
    @classmethod
    def kafka_servers_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS cannot be empty.")
        return v.strip()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Returns the singleton Settings instance.
    Cached after first call — settings are loaded once at startup.
    Use get_settings() everywhere instead of Settings() directly.

    Usage:
        from config.settings import get_settings
        settings = get_settings()
        namespace = settings.k8s_namespace
    """
    return Settings()