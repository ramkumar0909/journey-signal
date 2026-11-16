from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field


class ScenarioConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(max_length=100)
    count: int = Field(default=1000, ge=1, le=100000)
    fault_profile: Literal["normal", "faulty", "known"] = "faulty"
    fault_rate: float = Field(default=0.15, ge=0, le=1)


def read_scenario(path: Path) -> ScenarioConfig:
    if path.stat().st_size > 65536:
        raise ValueError("Scenario file too large")
    return ScenarioConfig.model_validate(yaml.safe_load(path.read_text()))


def start_timestamp(value: str | None) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if value else datetime.now(UTC)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Start time must include a timezone")
    return parsed.astimezone(UTC)
