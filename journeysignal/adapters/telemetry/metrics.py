from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.receipts = Counter(
            "journeysignal_receipts",
            "Handled delivery attempts",
            ["disposition"],
            registry=self.registry,
        )
        self.findings = Counter(
            "journeysignal_findings", "New durable findings", ["rule_id"], registry=self.registry
        )
        self.retries = Counter(
            "journeysignal_db_retries", "Transient DB retries", registry=self.registry
        )
        self.processing = Histogram(
            "journeysignal_processing_seconds",
            "Receive to durable DB return, excluding broker wait",
            registry=self.registry,
        )
        self.event_age = Histogram(
            "journeysignal_event_age_seconds",
            "Event to receive delay (negative ages excluded)",
            buckets=(0, 1, 5, 30, 60, 300, 600, 3600),
            registry=self.registry,
        )
        self.heartbeat = Gauge(
            "journeysignal_worker_heartbeat_timestamp_seconds",
            "Last successful worker poll",
            registry=self.registry,
        )
        self.ready = Gauge(
            "journeysignal_worker_ready", "Worker dependency readiness", registry=self.registry
        )
        self.lag = Gauge(
            "journeysignal_partition_lag",
            "Cached high watermark minus processed offset",
            ["partition"],
            registry=self.registry,
        )


metrics = Metrics()
