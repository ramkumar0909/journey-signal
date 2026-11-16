from dataclasses import dataclass
from datetime import datetime

from journeysignal.application.ports import EventParserPort, TransactionFactory
from journeysignal.domain.clocks import Clock
from journeysignal.domain.events import TransportRecord
from journeysignal.domain.findings import QualityOutcome
from journeysignal.domain.journeys import JourneyDependencyRegistry
from journeysignal.domain.rules import QualityRule, RuleContext


@dataclass(frozen=True)
class ProcessResult:
    replay: bool
    outcome: QualityOutcome | None


class IngestEvent:
    def __init__(
        self,
        transactions: TransactionFactory,
        parser: EventParserPort,
        clock: Clock,
        rules: tuple[QualityRule, ...],
        journeys: JourneyDependencyRegistry,
    ) -> None:
        self.transactions = transactions
        self.parser = parser
        self.clock = clock
        self.rules = rules
        self.journeys = journeys

    def execute(
        self, record: TransportRecord, received_at: datetime | None = None
    ) -> ProcessResult:
        received_at = received_at or self.clock.now()
        if received_at.tzinfo is None or received_at.utcoffset() is None:
            raise ValueError("Receive clock must be timezone aware")
        with self.transactions() as transaction:
            if not transaction.claim_receipt(record.source_ref, received_at):
                transaction.commit()
                return ProcessResult(True, None)
            parsed = self.parser.parse(record.value)
            first_digest = transaction.claim_business_id(parsed.event) if parsed.event else None
            context = RuleContext(
                record.source_ref, received_at, parsed.event, parsed.failure, first_digest
            )
            findings = tuple(f for rule in self.rules for f in rule.evaluate(context))
            outcome = QualityOutcome(
                record.source_ref,
                received_at,
                parsed.event,
                parsed.failure,
                findings,
                self.journeys.associate(findings),
            )
            transaction.save(outcome)
            transaction.commit()
            return ProcessResult(False, outcome)
