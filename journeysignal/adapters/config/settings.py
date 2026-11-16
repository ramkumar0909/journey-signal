"""Validated process configuration; secrets are never logged."""

from pathlib import Path

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="JS_", env_file=".env", extra="ignore", hide_input_in_errors=True
    )

    database_url: SecretStr
    api_database_url: SecretStr | None = None
    migration_database_url: SecretStr | None = None
    journey_config_path: Path | None = None
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    kafka_topic: str = "journeysignal.events.v1"
    kafka_group_id: str = "journeysignal-worker-v1"
    freshness_seconds: int = Field(default=300, ge=1, le=86400)
    future_skew_seconds: int = Field(default=30, ge=0, le=3600)
    max_record_bytes: int = Field(default=65536, ge=1024, le=1048576)
    max_json_depth: int = Field(default=8, ge=1, le=32)
    disabled_rules: frozenset[str] = frozenset()
    worker_metrics_port: int = Field(default=9101, ge=1024, le=65535)
    metrics_bind_host: str = "127.0.0.1"

    @field_validator("database_url", "api_database_url", "migration_database_url")
    @classmethod
    def postgres_url(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None and not value.get_secret_value().startswith("postgresql+psycopg://"):
            raise ValueError("PostgreSQL psycopg URL required")
        return value

    @field_validator("disabled_rules")
    @classmethod
    def known_disabled_rules(cls, value: frozenset[str]) -> frozenset[str]:
        from journeysignal.domain.rules import RULE_IDS

        if value - RULE_IDS:
            raise ValueError("Unknown disabled rule ID")
        return value
