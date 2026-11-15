"""Operational schema; only validated, allowlisted event data is durable."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TransportReceipt(Base):
    __tablename__ = "processed_transport_records"
    __table_args__ = (
        UniqueConstraint("topic", "partition", "offset", name="uq_transport_identity"),
        Index("ix_receipt_received", "received_at"),
    )
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    topic: Mapped[str] = mapped_column(String(249))
    partition: Mapped[int] = mapped_column(Integer)
    offset: Mapped[int] = mapped_column(BigInteger)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    disposition: Mapped[str] = mapped_column(String(24))
    failure_code: Mapped[str | None] = mapped_column(String(48))


class EventIdentity(Base):
    __tablename__ = "event_id_registry"
    event_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    first_fingerprint: Mapped[str] = mapped_column(String(64))
    first_receipt_id: Mapped[UUID] = mapped_column(ForeignKey(TransportReceipt.id))


class CanonicalEvent(Base):
    __tablename__ = "canonical_events"
    __table_args__ = (Index("ix_event_business_id", "event_id"),)
    receipt_id: Mapped[UUID] = mapped_column(ForeignKey(TransportReceipt.id), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(100))
    event_type: Mapped[str] = mapped_column(String(32))
    schema_version: Mapped[str] = mapped_column(String(16))
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    produced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    source: Mapped[str] = mapped_column(String(64))
    customer_id: Mapped[str | None] = mapped_column(String(80))
    session_id: Mapped[str | None] = mapped_column(String(80))
    fingerprint: Mapped[str] = mapped_column(String(64))
    properties: Mapped[dict[str, str | int]] = mapped_column(JSONB)


class FindingRow(Base):
    __tablename__ = "quality_findings"
    __table_args__ = (
        Index("ix_finding_time", "detected_at", "id"),
        Index("ix_finding_rule_time", "rule_id", "detected_at"),
        Index("ix_finding_event", "event_id"),
        UniqueConstraint("receipt_id", "rule_id", name="uq_receipt_rule"),
    )
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    receipt_id: Mapped[UUID] = mapped_column(ForeignKey(TransportReceipt.id))
    rule_id: Mapped[str] = mapped_column(String(16))
    category: Mapped[str] = mapped_column(String(32))
    severity: Mapped[str] = mapped_column(String(16))
    event_id: Mapped[str | None] = mapped_column(String(100))
    event_type: Mapped[str | None] = mapped_column(String(32))
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    evidence: Mapped[dict[str, str | int | bool]] = mapped_column(JSONB)
    message: Mapped[str] = mapped_column(String(300))
    suggested_action: Mapped[str] = mapped_column(String(300))


class ExposureRow(Base):
    __tablename__ = "journey_exposures"
    __table_args__ = (Index("ix_exposure_journey", "journey_id", "finding_id"),)
    finding_id: Mapped[UUID] = mapped_column(ForeignKey(FindingRow.id), primary_key=True)
    journey_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    dependency_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    reason: Mapped[str] = mapped_column(String(600))
