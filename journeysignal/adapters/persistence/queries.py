"""Bounded PostgreSQL read adapter; aggregations stay in SQL."""

from uuid import UUID

from sqlalchemy import Engine, and_, exists, func, or_, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from journeysignal.adapters.persistence.models import (
    CanonicalEvent,
    ExposureRow,
    FindingRow,
    TransportReceipt,
)
from journeysignal.application.queries import (
    FindingFilter,
    FindingItem,
    FindingPage,
    ImpactCounts,
    Overview,
    TimeWindow,
    decode_cursor,
    encode_cursor,
)
from journeysignal.domain.events import SourceRef
from journeysignal.domain.findings import JourneyExposure


def conditions(filters: FindingFilter) -> list[ColumnElement[bool]]:
    clauses: list[ColumnElement[bool]] = []
    for column, value in (
        (FindingRow.rule_id, filters.rule_id),
        (FindingRow.severity, filters.severity),
        (FindingRow.event_id, filters.event_id),
    ):
        if value is not None:
            clauses.append(column == value)
    if filters.window:
        clauses.extend(
            [
                FindingRow.detected_at >= filters.window.from_time,
                FindingRow.detected_at < filters.window.to_time,
            ]
        )
    if filters.journey_id:
        clauses.append(
            exists(
                select(ExposureRow.finding_id).where(
                    ExposureRow.finding_id == FindingRow.id,
                    ExposureRow.journey_id == filters.journey_id,
                )
            )
        )
    return clauses


class PostgresQueries:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def ready(self) -> bool:
        try:
            with self.engine.connect() as connection:
                return bool(
                    connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                    == "d826d256bae6"
                )
        except SQLAlchemyError:
            return False

    def overview(self, window: TimeWindow) -> Overview:
        r = TransportReceipt
        scope = [r.received_at >= window.from_time, r.received_at < window.to_time]
        with Session(self.engine) as session:
            total = session.scalar(select(func.count()).select_from(r).where(*scope)) or 0
            parsed = (
                session.scalar(
                    select(func.count())
                    .select_from(r)
                    .join(CanonicalEvent, CanonicalEvent.receipt_id == r.id)
                    .where(*scope)
                )
                or 0
            )
            valid = (
                session.scalar(
                    select(func.count())
                    .select_from(r)
                    .join(CanonicalEvent, CanonicalEvent.receipt_id == r.id)
                    .where(
                        *scope, ~exists(select(FindingRow.id).where(FindingRow.receipt_id == r.id))
                    )
                )
                or 0
            )
            bad_json = (
                session.scalar(
                    select(func.count())
                    .select_from(r)
                    .where(
                        *scope,
                        r.failure_code.in_(["invalid_json", "object_required", "null_record"]),
                    )
                )
                or 0
            )
            grouped: list[dict[str, int]] = []
            for column in (FindingRow.rule_id, FindingRow.severity, FindingRow.event_type):
                rows = session.execute(
                    select(column, func.count())
                    .where(
                        FindingRow.detected_at >= window.from_time,
                        FindingRow.detected_at < window.to_time,
                    )
                    .group_by(column)
                )
                grouped.append({str(key or "unknown"): count for key, count in rows})
            age = func.extract("epoch", r.received_at - CanonicalEvent.event_time)
            p50, p95 = session.execute(
                select(
                    func.percentile_cont(0.5).within_group(age),
                    func.percentile_cont(0.95).within_group(age),
                )
                .select_from(r)
                .join(CanonicalEvent, CanonicalEvent.receipt_id == r.id)
                .where(*scope)
            ).one()
            return Overview(
                window,
                total,
                parsed,
                valid,
                total - parsed,
                bad_json,
                valid / parsed if parsed else None,
                grouped[0],
                grouped[1],
                grouped[2],
                float(p50) if p50 is not None else None,
                float(p95) if p95 is not None else None,
            )

    def findings(self, filters: FindingFilter, limit: int, cursor: str | None) -> FindingPage:
        if not 1 <= limit <= 100:
            raise ValueError("Limit must be between 1 and 100")
        clauses = conditions(filters)
        if cursor:
            detected_at, finding_id = decode_cursor(cursor, filters)
            clauses.append(
                or_(
                    FindingRow.detected_at > detected_at,
                    and_(FindingRow.detected_at == detected_at, FindingRow.id > finding_id),
                )
            )
        with Session(self.engine) as session:
            rows = list(
                session.execute(
                    select(FindingRow, TransportReceipt)
                    .join(TransportReceipt, TransportReceipt.id == FindingRow.receipt_id)
                    .where(*clauses)
                    .order_by(FindingRow.detected_at, FindingRow.id)
                    .limit(limit + 1)
                )
            )
            selected = rows[:limit]
            exposures: dict[UUID, list[JourneyExposure]] = {}
            if selected:
                for exposure in session.scalars(
                    select(ExposureRow)
                    .where(ExposureRow.finding_id.in_([finding.id for finding, _ in selected]))
                    .order_by(ExposureRow.journey_id, ExposureRow.dependency_id)
                ):
                    exposures.setdefault(exposure.finding_id, []).append(
                        JourneyExposure(
                            exposure.journey_id,
                            exposure.finding_id,
                            exposure.dependency_id,
                            exposure.reason,
                        )
                    )
            items = tuple(
                FindingItem(
                    finding.id,
                    finding.rule_id,
                    finding.category,
                    finding.severity,
                    finding.event_id,
                    finding.event_type,
                    SourceRef(receipt.topic, receipt.partition, receipt.offset),
                    finding.detected_at,
                    finding.evidence,
                    finding.message,
                    finding.suggested_action,
                    tuple(exposures.get(finding.id, [])),
                )
                for finding, receipt in selected
            )
            next_cursor = (
                encode_cursor(items[-1].detected_at, items[-1].finding_id, filters)
                if len(rows) > limit
                else None
            )
            return FindingPage(items, next_cursor)

    def impact_counts(self, journey_id: str, window: TimeWindow) -> ImpactCounts:
        with Session(self.engine) as session:
            receipts, findings = session.execute(
                select(
                    func.count(func.distinct(FindingRow.receipt_id)),
                    func.count(FindingRow.id),
                ).where(*conditions(FindingFilter(journey_id=journey_id, window=window)))
            ).one()
            return ImpactCounts(receipts, findings)
