"""Allowlisted log fields, never raw payloads, credentials or exception messages."""

import json
import logging
from datetime import UTC, datetime


class SafeJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "level": record.levelname,
                "code": getattr(record, "code", "service_log"),
                **{
                    key: getattr(record, key)
                    for key in ("partition", "offset", "attempt")
                    if hasattr(record, key)
                },
            }
        )


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(SafeJsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
