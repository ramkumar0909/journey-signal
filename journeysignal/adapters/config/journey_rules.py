from importlib.resources import files
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from journeysignal.domain.events import EventType
from journeysignal.domain.journeys import JourneyDependency, JourneyDependencyRegistry
from journeysignal.domain.rules import RULE_IDS

ALLOWED_FIELDS = {"event_id", "event_time", "customer_id", "properties.cart_id"}


class JourneyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    dependency_id: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    display_name: str = Field(min_length=1, max_length=100)
    dependency_description: str = Field(min_length=1, max_length=300)
    required_event_types: tuple[EventType, ...] = Field(min_length=1)
    required_fields: tuple[str, ...] = Field(min_length=1)
    relevant_rule_ids: tuple[str, ...] = Field(min_length=1)
    freshness_threshold_seconds: int = Field(ge=1, le=86400)
    owner_role: str = Field(min_length=1, max_length=80)

    @field_validator("relevant_rule_ids")
    @classmethod
    def known_rules(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if set(value) - RULE_IDS:
            raise ValueError("Unknown journey rule")
        return value

    @field_validator("required_fields")
    @classmethod
    def known_fields(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if set(value) - ALLOWED_FIELDS:
            raise ValueError("Unknown journey field")
        return value


class RegistryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    journeys: tuple[JourneyConfig, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_ids(self) -> "RegistryConfig":
        if len({j.id for j in self.journeys}) != len(self.journeys):
            raise ValueError("Duplicate journey ID")
        if len({j.dependency_id for j in self.journeys}) != len(self.journeys):
            raise ValueError("Duplicate dependency ID")
        return self


def load_registry(path: Path | None = None) -> JourneyDependencyRegistry:
    content = (
        path.read_text()
        if path
        else files("journeysignal.adapters.config").joinpath("journeys.yaml").read_text()
    )
    if len(content) > 65536:
        raise ValueError("Journey configuration too large")
    config = RegistryConfig.model_validate(yaml.safe_load(content))
    return JourneyDependencyRegistry(
        tuple(
            JourneyDependency(
                j.id,
                j.dependency_id,
                j.display_name,
                j.dependency_description,
                j.required_event_types,
                j.required_fields,
                j.relevant_rule_ids,
                j.freshness_threshold_seconds,
                j.owner_role,
            )
            for j in config.journeys
        )
    )
