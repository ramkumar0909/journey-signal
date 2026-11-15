from datetime import UTC, datetime
from types import TracebackType
from uuid import UUID, uuid4

from sqlalchemy import Engine, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from journeysignal.adapters.persistence.models import (
    CanonicalEvent,
    EventIdentity,
    ExposureRow,
    FindingRow,
    TransportReceipt,
)
from journeysignal.domain.events import Event, SourceRef
from journeysignal.domain.findings import QualityOutcome


class PostgresTransaction:
    def __init__(self, engine: Engine) -> None:
        self.session = Session(engine)
        self.receipt_id: UUID | None = None
        self.saved = False

    def __enter__(self) -> "PostgresTransaction":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.session.close()  # rolls back any transaction not explicitly committed

    def claim_receipt(self, source_ref: SourceRef, received_at: datetime) -> bool:
        if self.receipt_id is not None:
            raise RuntimeError("Only one new receipt per transaction")
        candidate = uuid4()
        self.receipt_id = self.session.execute(
            insert(TransportReceipt)
            .values(
                id=candidate,
                topic=source_ref.topic,
                partition=source_ref.partition,
                offset=source_ref.offset,
                received_at=received_at,
                processed_at=datetime.now(UTC),
                disposition="processing",
            )
            .on_conflict_do_nothing(constraint="uq_transport_identity")
            .returning(TransportReceipt.id)
        ).scalar_one_or_none()
        return self.receipt_id is not None

    def claim_business_id(self, event: Event) -> str | None:
        if self.receipt_id is None:
            raise RuntimeError("Claim a receipt before business identity")
        inserted = self.session.execute(
            insert(EventIdentity)
            .values(
                event_id=event.event_id,
                first_fingerprint=event.fingerprint,
                first_receipt_id=self.receipt_id,
            )
            .on_conflict_do_nothing(index_elements=[EventIdentity.event_id])
            .returning(EventIdentity.event_id)
        ).scalar_one_or_none()
        if inserted is not None:
            return None
        return self.session.execute(
            select(EventIdentity.first_fingerprint).where(EventIdentity.event_id == event.event_id)
        ).scalar_one()

    def save(self, outcome: QualityOutcome) -> None:
        if self.receipt_id is None:
            raise RuntimeError("Claim a receipt before saving an outcome")
        receipt = self.session.get(TransportReceipt, self.receipt_id)
        if receipt is None:
            raise RuntimeError("Claimed receipt is missing")
        if outcome.source_ref != SourceRef(receipt.topic, receipt.partition, receipt.offset):
            raise RuntimeError("Outcome does not match claimed receipt")
        receipt.disposition = (
            "quarantined" if outcome.failure else ("flagged" if outcome.findings else "accepted")
        )
        receipt.failure_code = outcome.failure.code if outcome.failure else None
        receipt.processed_at = datetime.now(UTC)
        event = outcome.event
        if event:
            self.session.add(
                CanonicalEvent(
                    receipt_id=self.receipt_id,
                    event_id=event.event_id,
                    event_type=event.event_type.value,
                    schema_version=event.schema_version,
                    event_time=event.event_time,
                    produced_at=event.produced_at,
                    source=event.source,
                    customer_id=event.customer_id,
                    session_id=event.session_id,
                    fingerprint=event.fingerprint,
                    properties=dict(event.properties),
                )
            )
        for finding in outcome.findings:
            self.session.add(
                FindingRow(
                    id=finding.finding_id,
                    receipt_id=self.receipt_id,
                    rule_id=finding.rule_id,
                    category=finding.category.value,
                    severity=finding.severity.value,
                    event_id=finding.event_id,
                    event_type=finding.event_type.value if finding.event_type else None,
                    detected_at=finding.detected_at,
                    evidence=dict(finding.evidence),
                    message=finding.message,
                    suggested_action=finding.suggested_action,
                )
            )
        self.session.flush()  # finding FKs exist before exposure insertion
        for exposure in outcome.exposures:
            self.session.add(
                ExposureRow(
                    finding_id=exposure.finding_id,
                    journey_id=exposure.journey_id,
                    dependency_id=exposure.dependency_id,
                    reason=exposure.reason,
                )
            )
        self.saved = True

    def commit(self) -> None:
        if self.receipt_id is not None and not self.saved:
            raise RuntimeError("Cannot commit an incomplete receipt outcome")
        self.session.commit()
