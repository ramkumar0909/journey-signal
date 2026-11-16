"""Dependency composition, never quality business logic."""

from sqlalchemy import Engine, create_engine

from journeysignal.adapters.config.journey_rules import load_registry
from journeysignal.adapters.config.settings import Settings
from journeysignal.adapters.kafka.serialization import EventParser
from journeysignal.adapters.persistence.unit_of_work import PostgresTransaction
from journeysignal.application.ingest_event import IngestEvent
from journeysignal.domain.clocks import Clock, SystemClock
from journeysignal.domain.rules import quality_rules


def load_settings() -> Settings:
    return Settings()


def make_engine(settings: Settings) -> Engine:
    return create_engine(
        settings.database_url.get_secret_value(),
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
        connect_args={"connect_timeout": 3, "options": "-cstatement_timeout=10000"},
    )


def build_ingester(settings: Settings, engine: Engine, clock: Clock | None = None) -> IngestEvent:
    registry = load_registry(settings.journey_config_path)
    if any(j.freshness_threshold_seconds != settings.freshness_seconds for j in registry.journeys):
        raise ValueError("Journey freshness threshold must match the active M1 freshness policy")
    return IngestEvent(
        lambda: PostgresTransaction(engine),
        EventParser(settings.max_record_bytes, settings.max_json_depth),
        clock or SystemClock(),
        quality_rules(
            settings.freshness_seconds, settings.future_skew_seconds, settings.disabled_rules
        ),
        registry,
    )
